// ============================================================================
// File: poolstate.rs
// Location: snap-coin-pool-v2/src/poolstate.rs
// Version: 1.2.1-miner-shares-persist.1
//
// Description: Server-side shared pool state (snapshot loading) + lightweight
//              disk persistence (no DB).
//
//              Provides:
//                - Overwritten JSON snapshot file: pool_state.json (atomic write)
//                - Optional append-only payout ledger: payout_ledger_YYYY-MM-DD.jsonl
//
//              Intended integration points (from pool_stats_server.rs):
//                - pool_state.on_event(&PoolEvent)  (for EVERY emitted event)
//                - pool_state.record_hashrate_sample(f64) (periodic sample; e.g. 60s)
//                - GET /api/snapshot -> pool_state.snapshot()
//
// Notes:
//   - Recent events are split into THREE independent bounded buffers:
//       * recent_shares  (default 200)   - share events + other non-block/payout events
//       * recent_blocks  (default 200)   - BlockFound events
//       * recent_payouts (default 200)   - PayoutComplete events
//   - Each buffer has its own cap.
//   - NetworkStats is NOT stored in any recent_* buffer, but its hashrate value
//     IS recorded into a dedicated 24h timeseries for frontend clarity.
//   - Daily buckets bounded (default 30 days).
//   - Uses internal UTC day formatter (civil_from_days) to avoid chrono.
//   - Persistent timeseries stored inside pool_state.json.
//   - Per-miner payout totals rebuilt from payout_ledger_*.jsonl on boot.
//
// CHANGELOG (v1.2.1-miner-shares-persist.1):
//   - ADD: miner_shares_acc (HashMap<String, u64>) to PoolSnapshot.
//   - ADD: miner_shares_rej (HashMap<String, u64>) to PoolSnapshot.
//   - ADD: on_event(ShareAccepted/ShareRejected) accumulates per-miner counts.
//   - ADD: Per-miner share counts persisted in pool_state.json across restarts.
//
// PRIOR CHANGELOG (v1.2.0-miner-paid-totals.1):
//   - ADD: MinerLastPayout struct for per-miner last payout info.
//   - ADD: miner_paid_totals (HashMap<String, u64>) to PoolSnapshot.
//   - ADD: miner_last_payout (HashMap<String, MinerLastPayout>) to PoolSnapshot.
//   - ADD: On boot, scan payout_ledger_*.jsonl to rebuild per-miner maps.
//   - ADD: on_event(PayoutComplete) updates in-memory per-miner maps.
//   - ADD: snapshot() includes per-miner maps in response.
//
// PRIOR CHANGELOG (v1.1.6-timeseries-dual-hashrate24h.1):
//   - ADD: Persist TWO distinct 24h hashrate series into pool_state.json.
//   - KEEP: NetworkStats stays out of recent event buffers.
//   - REMOVE: any hashrate "cap/spike filter" behavior (not requested).
// ============================================================================

use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::pool_stats_server::PoolEvent;
use num_bigint::BigUint;

const DEFAULT_STATE_FILE: &str = "pool_state.json";
const DEFAULT_LEDGER_DIR: &str = ".";
const DEFAULT_DAILY_DAYS: usize = 30;

// Per-buffer independent caps.
const DEFAULT_RECENT_SHARES: usize = 200;
const DEFAULT_RECENT_BLOCKS: usize = 200;
const DEFAULT_RECENT_PAYOUTS: usize = 200;

// Timeseries caps.
const DEFAULT_HASHRATE_POINTS_24H_1M: usize = 5760; // 24h @ 1 sample/15seconds

// blocks_1d hourly points (24 hours)
const DEFAULT_BLOCKS_1D_HOURLY_POINTS: i64 = 24;

// ── Env parsing helpers ─────────────────────────────────────────────────────

#[derive(Clone, Debug, Deserialize)]
struct EnvPoolDifficulty {
    #[serde(with = "serde_bytes_32")]
    bytes: [u8; 32],
}

mod serde_bytes_32 {
    use serde::{Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = Vec::<u8>::deserialize(deserializer)?;
        if v.len() != 32 {
            return Err(serde::de::Error::custom(format!(
                "expected 32 bytes, got {}",
                v.len()
            )));
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(&v);
        Ok(out)
    }
}

// ── Snapshot types ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct DailyBucket {
    pub date: String, // "YYYY-MM-DD" (UTC)
    pub blocks: u64,
    pub paid: u64, // atomic units
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct Totals {
    pub blocks_found: u64,
    pub shares_acc: u64,
    pub shares_rej: u64,
    pub total_paid_to_miners: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct LastBlock {
    pub height: u64,
    pub hash: String,
    pub timestamp: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct LastPayout {
    pub height: u64,
    pub miners_paid: u64,
    pub total_reward: u64,
    pub pool_fee: u64,
    pub paid_to_miners: u64,
    pub txid: String,
    pub timestamp: u64,
}

// ── Per-miner payout tracking ───────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct MinerLastPayout {
    pub amount: u64,
    #[serde(default)]
    pub height: u64,
    #[serde(default)]
    pub ts: u64,
}

// ── Persistent time series ──────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TimeSeriesPoint {
    pub t: u64, // unix seconds (bucket start, UTC)
    pub v: f64, // numeric value
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct TimeSeries {
    // 24h rolling buffers sampled (typically) at 1 minute cadence.
    //
    // NOTE: alias keeps backward-compat with any older state files that used
    //       timeseries.hashrate_1m_24h (it will land in pool_hashrate_1m_24h).
    #[serde(default, alias = "hashrate_1m_24h")]
    pub pool_hashrate_1m_24h: Vec<TimeSeriesPoint>,

    #[serde(default)]
    pub network_hashrate_1m_24h: Vec<TimeSeriesPoint>,

    // HOURLY block count buckets, derived from recent_blocks timestamps.
    // Filled with 0 for missing hours.
    #[serde(default)]
    pub blocks_1d: Vec<TimeSeriesPoint>, // length 24 (hour buckets)

    // DAILY block count buckets, derived from daily_buckets (fills missing days w/0).
    // These are persisted for dumb frontend consumption.
    #[serde(default)]
    pub blocks_7d: Vec<TimeSeriesPoint>, // length 7
    #[serde(default)]
    pub blocks_30d: Vec<TimeSeriesPoint>, // length 30
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PoolSnapshot {
    pub generated_at: u64,
    pub totals: Totals,
    pub last_block: LastBlock,

    #[serde(default)]
    pub last_payout: LastPayout,

    pub daily_buckets: Vec<DailyBucket>,

    // NEW schema: 3 separate lists
    #[serde(default)]
    pub recent_shares: Vec<PoolEvent>,
    #[serde(default)]
    pub recent_blocks: Vec<PoolEvent>,
    #[serde(default)]
    pub recent_payouts: Vec<PoolEvent>,

    #[serde(default)]
    pub pool_difficulty_fixed: String,

    #[serde(default)]
    pub pool_difficulty_fixed_num: u64,

    // Persistent timeseries
    #[serde(default)]
    pub timeseries: TimeSeries,

    // Per-miner payout tracking (rebuilt from ledger on boot)
    #[serde(default)]
    pub miner_paid_totals: HashMap<String, u64>,

    #[serde(default)]
    pub miner_last_payout: HashMap<String, MinerLastPayout>,

    // Per-miner share counts (persisted in pool_state.json)
    #[serde(default)]
    pub miner_shares_acc: HashMap<String, u64>,

    #[serde(default)]
    pub miner_shares_rej: HashMap<String, u64>,
}

// Legacy schema loader (pre split): recent_events
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct LegacyPoolSnapshot {
    pub generated_at: u64,
    pub totals: Totals,
    pub last_block: LastBlock,

    #[serde(default)]
    pub last_payout: LastPayout,

    #[serde(default)]
    pub daily_buckets: Vec<DailyBucket>,

    #[serde(default)]
    pub recent_events: Vec<PoolEvent>,

    #[serde(default)]
    pub pool_difficulty_fixed: String,

    #[serde(default)]
    pub pool_difficulty_fixed_num: u64,
}

#[derive(Debug)]
struct Inner {
    snapshot: PoolSnapshot,

    recent_shares: VecDeque<PoolEvent>,
    recent_blocks: VecDeque<PoolEvent>,
    recent_payouts: VecDeque<PoolEvent>,

    // Timeseries in-memory deques
    pool_hashrate_1m_24h: VecDeque<TimeSeriesPoint>,
    network_hashrate_1m_24h: VecDeque<TimeSeriesPoint>,

    // Per-miner payout accumulators (in-memory, rebuilt from ledger on boot)
    miner_paid_totals: HashMap<String, u64>,
    miner_last_payout: HashMap<String, MinerLastPayout>,

    // Per-miner share counts (in-memory, persisted via pool_state.json)
    miner_shares_acc: HashMap<String, u64>,
    miner_shares_rej: HashMap<String, u64>,

    dirty: bool,
    state_file: PathBuf,

    ledger_enable: bool,
    ledger_dir: PathBuf,
}

#[derive(Clone)]
pub struct PoolState {
    inner: Arc<Mutex<Inner>>,
}

impl PoolState {
    pub async fn new_from_env() -> Self {
        let _ = dotenvy::dotenv();

        let state_file =
            std::env::var("POOL_STATE_FILE").unwrap_or_else(|_| DEFAULT_STATE_FILE.to_string());
        let ledger_dir =
            std::env::var("POOL_LEDGER_DIR").unwrap_or_else(|_| DEFAULT_LEDGER_DIR.to_string());
        let ledger_enable = std::env::var("POOL_LEDGER_ENABLE")
            .ok()
            .map(|v| v != "0")
            .unwrap_or(true);

        let state_path = PathBuf::from(state_file);

        let mut snapshot = PoolSnapshot::default();
        let mut legacy_seed: Vec<PoolEvent> = Vec::new();

        if let Ok(bytes) = tokio::fs::read(&state_path).await {
            if let Ok(mut loaded_new) = serde_json::from_slice::<PoolSnapshot>(&bytes) {
                loaded_new.recent_shares.retain(|e| !is_network_stats(e));
                loaded_new.recent_blocks.retain(|e| !is_network_stats(e));
                loaded_new.recent_payouts.retain(|e| !is_network_stats(e));
                snapshot = loaded_new;
            } else if let Ok(mut loaded_old) = serde_json::from_slice::<LegacyPoolSnapshot>(&bytes) {
                loaded_old.recent_events.retain(|e| !is_network_stats(e));

                snapshot.generated_at = loaded_old.generated_at;
                snapshot.totals = loaded_old.totals;
                snapshot.last_block = loaded_old.last_block;
                snapshot.last_payout = loaded_old.last_payout;
                snapshot.daily_buckets = loaded_old.daily_buckets;
                snapshot.pool_difficulty_fixed = loaded_old.pool_difficulty_fixed;
                snapshot.pool_difficulty_fixed_num = loaded_old.pool_difficulty_fixed_num;

                legacy_seed = loaded_old.recent_events;
            }
        }

        snapshot.generated_at = now_ts();

        if snapshot.pool_difficulty_fixed.trim().is_empty() {
            snapshot.pool_difficulty_fixed =
                read_pool_difficulty_fixed_hex_from_env().unwrap_or_else(|| "-".to_string());
        }
        if snapshot.pool_difficulty_fixed_num == 0 {
            snapshot.pool_difficulty_fixed_num =
                compute_fixed_difficulty_u64_from_env().unwrap_or(0);
        }

        let mut recent_shares = VecDeque::with_capacity(DEFAULT_RECENT_SHARES);
        let mut recent_blocks = VecDeque::with_capacity(DEFAULT_RECENT_BLOCKS);
        let mut recent_payouts = VecDeque::with_capacity(DEFAULT_RECENT_PAYOUTS);

        if !legacy_seed.is_empty() {
            for e in legacy_seed.into_iter() {
                if is_network_stats(&e) {
                    continue;
                }
                match &e {
                    PoolEvent::BlockFound { .. } => {
                        recent_blocks.push_back(e);
                        while recent_blocks.len() > DEFAULT_RECENT_BLOCKS {
                            recent_blocks.pop_front();
                        }
                    }
                    PoolEvent::PayoutComplete { .. } => {
                        recent_payouts.push_back(e);
                        while recent_payouts.len() > DEFAULT_RECENT_PAYOUTS {
                            recent_payouts.pop_front();
                        }
                    }
                    _ => {
                        recent_shares.push_back(e);
                        while recent_shares.len() > DEFAULT_RECENT_SHARES {
                            recent_shares.pop_front();
                        }
                    }
                }
            }
        } else {
            for e in snapshot.recent_shares.iter().cloned().take(DEFAULT_RECENT_SHARES) {
                recent_shares.push_back(e);
            }
            for e in snapshot.recent_blocks.iter().cloned().take(DEFAULT_RECENT_BLOCKS) {
                recent_blocks.push_back(e);
            }
            for e in snapshot
                .recent_payouts
                .iter()
                .cloned()
                .take(DEFAULT_RECENT_PAYOUTS)
            {
                recent_payouts.push_back(e);
            }
        }

        if snapshot.last_payout.height == 0 {
            rebuild_last_payout_from_recent_payouts(&mut snapshot, &recent_payouts);
        }

        snapshot.recent_shares = recent_shares.iter().cloned().collect();
        snapshot.recent_blocks = recent_blocks.iter().cloned().collect();
        snapshot.recent_payouts = recent_payouts.iter().cloned().collect();

        // ── Timeseries load (pool hashrate) ──────────────────────────────────
        let mut pool_hashrate_1m_24h = VecDeque::with_capacity(DEFAULT_HASHRATE_POINTS_24H_1M);
        if !snapshot.timeseries.pool_hashrate_1m_24h.is_empty() {
            for p in snapshot
                .timeseries
                .pool_hashrate_1m_24h
                .iter()
                .cloned()
                .take(DEFAULT_HASHRATE_POINTS_24H_1M)
            {
                pool_hashrate_1m_24h.push_back(p);
            }
            while pool_hashrate_1m_24h.len() > DEFAULT_HASHRATE_POINTS_24H_1M {
                pool_hashrate_1m_24h.pop_front();
            }
        }

        // ── Timeseries load (network hashrate) ───────────────────────────────
        let mut network_hashrate_1m_24h = VecDeque::with_capacity(DEFAULT_HASHRATE_POINTS_24H_1M);
        if !snapshot.timeseries.network_hashrate_1m_24h.is_empty() {
            for p in snapshot
                .timeseries
                .network_hashrate_1m_24h
                .iter()
                .cloned()
                .take(DEFAULT_HASHRATE_POINTS_24H_1M)
            {
                network_hashrate_1m_24h.push_back(p);
            }
            while network_hashrate_1m_24h.len() > DEFAULT_HASHRATE_POINTS_24H_1M {
                network_hashrate_1m_24h.pop_front();
            }
        }

        // ── Timeseries rebuild (blocks) ──────────────────────────────────────
        rebuild_block_timeseries_daily_from_daily(&mut snapshot);
        rebuild_block_timeseries_hourly_1d_from_recent_blocks(&mut snapshot, &recent_blocks);

        // ── Per-miner payout totals: rebuild from ledger files ───────────────
        let (miner_paid_totals, miner_last_payout) =
            rebuild_miner_totals_from_ledger(Path::new(&ledger_dir)).await;

        println!(
            "[poolstate] Rebuilt per-miner payout totals from ledger: {} miners tracked",
            miner_paid_totals.len()
        );

        // ── Per-miner share counts: load from persisted snapshot ─────────────
        let snapshot_shares_acc = snapshot.miner_shares_acc.clone();
        let snapshot_shares_rej = snapshot.miner_shares_rej.clone();

        println!(
            "[poolstate] Loaded per-miner share counts from state: {} miners with shares",
            snapshot_shares_acc.len()
        );

        let inner = Inner {
            snapshot,
            recent_shares,
            recent_blocks,
            recent_payouts,
            pool_hashrate_1m_24h,
            network_hashrate_1m_24h,
            miner_paid_totals,
            miner_last_payout,
            miner_shares_acc: snapshot_shares_acc,
            miner_shares_rej: snapshot_shares_rej,
            dirty: false,
            state_file: state_path,
            ledger_enable,
            ledger_dir: PathBuf::from(ledger_dir),
        };

        let this = Self {
            inner: Arc::new(Mutex::new(inner)),
        };

        this.spawn_flush_task();
        this
    }

    pub async fn snapshot(&self) -> PoolSnapshot {
        let mut g = self.inner.lock().await;

        g.snapshot.generated_at = now_ts();
        g.snapshot.recent_shares = g.recent_shares.iter().cloned().collect();
        g.snapshot.recent_blocks = g.recent_blocks.iter().cloned().collect();
        g.snapshot.recent_payouts = g.recent_payouts.iter().cloned().collect();

        // Timeseries into snapshot
        g.snapshot.timeseries.pool_hashrate_1m_24h = g.pool_hashrate_1m_24h.iter().cloned().collect();
        g.snapshot.timeseries.network_hashrate_1m_24h =
            g.network_hashrate_1m_24h.iter().cloned().collect();

        // Per-miner payout data into snapshot
        g.snapshot.miner_paid_totals = g.miner_paid_totals.clone();
        g.snapshot.miner_last_payout = g.miner_last_payout.clone();

        // Per-miner share counts into snapshot
        g.snapshot.miner_shares_acc = g.miner_shares_acc.clone();
        g.snapshot.miner_shares_rej = g.miner_shares_rej.clone();

        // Blocks series (ensure up-to-date even if client polls infrequently)
        rebuild_block_timeseries_daily_from_daily(&mut g.snapshot);

        // FIX: avoid E0502 by cloning recent_blocks first
        let recent_blocks = g.recent_blocks.clone();
        rebuild_block_timeseries_hourly_1d_from_recent_blocks(&mut g.snapshot, &recent_blocks);

        g.snapshot.clone()
    }

    pub async fn on_event(&self, event: &PoolEvent) {
        // NetworkStats: record ONLY into network hashrate timeseries.
        // Do not store in any recent_* buffer, and do not touch totals.
        if is_network_stats(event) {
            if let Some(hs) = extract_network_hashrate_hs(event) {
                let t = extract_event_ts_seconds(event).unwrap_or_else(now_ts);
                let mut g = self.inner.lock().await;

                g.network_hashrate_1m_24h.push_back(TimeSeriesPoint { t, v: hs });
                while g.network_hashrate_1m_24h.len() > DEFAULT_HASHRATE_POINTS_24H_1M {
                    g.network_hashrate_1m_24h.pop_front();
                }

                g.dirty = true;
            }
            return;
        }

        let mut g = self.inner.lock().await;

        // Borrow-safe routing: touch one deque at a time.
        match event {
            PoolEvent::BlockFound { .. } => {
                g.recent_blocks.push_back(event.clone());
                while g.recent_blocks.len() > DEFAULT_RECENT_BLOCKS {
                    g.recent_blocks.pop_front();
                }
            }
            PoolEvent::PayoutComplete { .. } => {
                g.recent_payouts.push_back(event.clone());
                while g.recent_payouts.len() > DEFAULT_RECENT_PAYOUTS {
                    g.recent_payouts.pop_front();
                }
            }
            _ => {
                g.recent_shares.push_back(event.clone());
                while g.recent_shares.len() > DEFAULT_RECENT_SHARES {
                    g.recent_shares.pop_front();
                }
            }
        }

        // Aggregates
        match event {
            PoolEvent::ShareAccepted { miner, .. } => {
                g.snapshot.totals.shares_acc = g.snapshot.totals.shares_acc.saturating_add(1);

                // Per-miner accumulation
                let entry = g.miner_shares_acc.entry(miner.clone()).or_insert(0);
                *entry = entry.saturating_add(1);
            }
            PoolEvent::ShareRejected { miner, .. } => {
                g.snapshot.totals.shares_rej = g.snapshot.totals.shares_rej.saturating_add(1);

                // Per-miner accumulation
                let entry = g.miner_shares_rej.entry(miner.clone()).or_insert(0);
                *entry = entry.saturating_add(1);
            }
            PoolEvent::BlockFound {
                height,
                hash,
                timestamp,
                ..
            } => {
                g.snapshot.totals.blocks_found = g.snapshot.totals.blocks_found.saturating_add(1);
                g.snapshot.last_block.height = *height;
                g.snapshot.last_block.hash = hash.clone();
                g.snapshot.last_block.timestamp = *timestamp;

                bump_daily_blocks(&mut g.snapshot.daily_buckets, *timestamp);
                truncate_daily(&mut g.snapshot.daily_buckets);

                // Rebuild derived blocks series for dumb frontend.
                rebuild_block_timeseries_daily_from_daily(&mut g.snapshot);

                // FIX: avoid E0502 by cloning recent_blocks first
                let recent_blocks = g.recent_blocks.clone();
                rebuild_block_timeseries_hourly_1d_from_recent_blocks(
                    &mut g.snapshot,
                    &recent_blocks,
                );
            }
            PoolEvent::PayoutComplete {
                height,
                miners_paid,
                total_reward,
                pool_fee,
                txid,
                payouts,
                timestamp,
                ..
            } => {
                let paid_to_miners = if !payouts.is_empty() {
                    payouts
                        .iter()
                        .fold(0u64, |acc, p| acc.saturating_add(p.amount))
                } else {
                    total_reward.saturating_sub(*pool_fee)
                };

                g.snapshot.totals.total_paid_to_miners = g
                    .snapshot
                    .totals
                    .total_paid_to_miners
                    .saturating_add(paid_to_miners);

                bump_daily_paid(&mut g.snapshot.daily_buckets, *timestamp, paid_to_miners);
                truncate_daily(&mut g.snapshot.daily_buckets);

                g.snapshot.last_payout = LastPayout {
                    height: *height,
                    miners_paid: *miners_paid as u64,
                    total_reward: *total_reward,
                    pool_fee: *pool_fee,
                    paid_to_miners,
                    txid: txid.clone(),
                    timestamp: *timestamp,
                };

                // ── Per-miner payout accumulation ────────────────────────────
                for p in payouts.iter() {
                    let miner_key = format!("{}", p.miner);

                    // Accumulate total
                    let entry = g.miner_paid_totals.entry(miner_key.clone()).or_insert(0);
                    *entry = entry.saturating_add(p.amount);

                    // Track last payout
                    g.miner_last_payout.insert(
                        miner_key,
                        MinerLastPayout {
                            amount: p.amount,
                            height: *height,
                            ts: *timestamp,
                        },
                    );
                }

                if g.ledger_enable {
                    let _ = append_ledger_line(&g.ledger_dir, *timestamp, event);
                }
            }
            _ => {}
        }

        g.dirty = true;
    }

    // ── Timeseries API ──────────────────────────────────────────────────────
    //
    // Call this from your stats loop (e.g. every 60s) with your computed pool
    // hashrate in H/s. This persists to pool_state.json and loads on boot.
    pub async fn record_hashrate_sample(&self, hashrate_hs: f64) {
        let mut g = self.inner.lock().await;

        g.pool_hashrate_1m_24h.push_back(TimeSeriesPoint {
            t: now_ts(),
            v: hashrate_hs,
        });

        while g.pool_hashrate_1m_24h.len() > DEFAULT_HASHRATE_POINTS_24H_1M {
            g.pool_hashrate_1m_24h.pop_front();
        }

        g.dirty = true;
    }

    fn spawn_flush_task(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;

                let mut g = this.inner.lock().await;
                if !g.dirty {
                    continue;
                }

                g.snapshot.generated_at = now_ts();
                g.snapshot.recent_shares = g.recent_shares.iter().cloned().collect();
                g.snapshot.recent_blocks = g.recent_blocks.iter().cloned().collect();
                g.snapshot.recent_payouts = g.recent_payouts.iter().cloned().collect();

                // Timeseries into snapshot
                g.snapshot.timeseries.pool_hashrate_1m_24h =
                    g.pool_hashrate_1m_24h.iter().cloned().collect();
                g.snapshot.timeseries.network_hashrate_1m_24h =
                    g.network_hashrate_1m_24h.iter().cloned().collect();

                // Per-miner payout data into snapshot
                g.snapshot.miner_paid_totals = g.miner_paid_totals.clone();
                g.snapshot.miner_last_payout = g.miner_last_payout.clone();

                // Per-miner share counts into snapshot
                g.snapshot.miner_shares_acc = g.miner_shares_acc.clone();
                g.snapshot.miner_shares_rej = g.miner_shares_rej.clone();

                // Derived blocks series
                rebuild_block_timeseries_daily_from_daily(&mut g.snapshot);

                // FIX: avoid E0502 by cloning recent_blocks first
                let recent_blocks = g.recent_blocks.clone();
                rebuild_block_timeseries_hourly_1d_from_recent_blocks(
                    &mut g.snapshot,
                    &recent_blocks,
                );

                let path = g.state_file.clone();
                let json = match serde_json::to_vec_pretty(&g.snapshot) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                let tmp = path.with_extension("json.tmp");
                let write_ok = tokio::task::spawn_blocking(move || -> bool {
                    if let Some(parent) = path.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    if std::fs::write(&tmp, &json).is_err() {
                        return false;
                    }
                    std::fs::rename(&tmp, &path).is_ok()
                })
                .await
                .unwrap_or(false);

                if write_ok {
                    g.dirty = false;
                }
            }
        });
    }
}

// ── Rebuild per-miner totals from payout ledger files ───────────────────────

/// Scans all payout_ledger_*.jsonl files in ledger_dir, parses each line as a
/// PayoutComplete event, and accumulates per-miner paid totals + last payout.
async fn rebuild_miner_totals_from_ledger(
    ledger_dir: &Path,
) -> (HashMap<String, u64>, HashMap<String, MinerLastPayout>) {
    let mut paid_totals: HashMap<String, u64> = HashMap::new();
    let mut last_payouts: HashMap<String, MinerLastPayout> = HashMap::new();

    // Collect ledger files
    let mut ledger_files: Vec<PathBuf> = Vec::new();
    if let Ok(mut entries) = tokio::fs::read_dir(ledger_dir).await {
        while let Ok(Some(entry)) = entries.next_entry().await {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("payout_ledger_") && name.ends_with(".jsonl") {
                    ledger_files.push(path);
                }
            }
        }
    }

    // Sort by filename so we process chronologically
    ledger_files.sort();

    let mut lines_ok: u64 = 0;
    let mut lines_err: u64 = 0;

    for path in &ledger_files {
        if let Ok(contents) = tokio::fs::read_to_string(path).await {
            for line in contents.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                match serde_json::from_str::<PoolEvent>(line) {
                    Ok(PoolEvent::PayoutComplete {
                        height,
                        payouts,
                        timestamp,
                        ..
                    }) => {
                        for p in payouts.iter() {
                            let miner_key = format!("{}", p.miner);

                            let entry = paid_totals.entry(miner_key.clone()).or_insert(0);
                            *entry = entry.saturating_add(p.amount);

                            last_payouts.insert(
                                miner_key,
                                MinerLastPayout {
                                    amount: p.amount,
                                    height,
                                    ts: timestamp,
                                },
                            );
                        }
                        lines_ok += 1;
                    }
                    Ok(_) => {
                        // Non-payout event in ledger (shouldn't happen, ignore)
                        lines_ok += 1;
                    }
                    Err(_) => {
                        lines_err += 1;
                    }
                }
            }
        }
    }

    if lines_ok > 0 || lines_err > 0 {
        println!(
            "[poolstate] Ledger scan: {} files, {} lines ok, {} lines err",
            ledger_files.len(),
            lines_ok,
            lines_err
        );
    }

    (paid_totals, last_payouts)
}

// ── helpers ────────────────────────────────────────────────────────────────

#[inline]
fn is_network_stats(e: &PoolEvent) -> bool {
    matches!(e, PoolEvent::NetworkStats { .. })
}

fn extract_event_ts_seconds(e: &PoolEvent) -> Option<u64> {
    let v = serde_json::to_value(e).ok()?;
    // try common keys
    if let Some(ts) = v.get("timestamp").and_then(|x| x.as_u64()) {
        return Some(ts);
    }
    if let Some(ts) = v.get("ts").and_then(|x| x.as_u64()) {
        return Some(ts);
    }
    None
}

fn extract_network_hashrate_hs(e: &PoolEvent) -> Option<f64> {
    let v = serde_json::to_value(e).ok()?;
    // try common key spellings for hashrate
    for k in [
        "network_hashrate_hs",
        "network_hashrate",
        "hashrate_hs",
        "hashrate",
        "net_hashrate_hs",
        "net_hashrate",
    ] {
        if let Some(x) = v.get(k) {
            if let Some(f) = x.as_f64() {
                return Some(f);
            }
            if let Some(u) = x.as_u64() {
                return Some(u as f64);
            }
            if let Some(i) = x.as_i64() {
                return Some(i as f64);
            }
        }
    }
    None
}

fn rebuild_last_payout_from_recent_payouts(
    snapshot: &mut PoolSnapshot,
    recent_payouts: &VecDeque<PoolEvent>,
) {
    for e in recent_payouts.iter().rev() {
        if let PoolEvent::PayoutComplete {
            height,
            miners_paid,
            total_reward,
            pool_fee,
            txid,
            payouts,
            timestamp,
            ..
        } = e
        {
            let paid_to_miners = if !payouts.is_empty() {
                payouts.iter().fold(0u64, |acc, p| acc.saturating_add(p.amount))
            } else {
                total_reward.saturating_sub(*pool_fee)
            };

            snapshot.last_payout = LastPayout {
                height: *height,
                miners_paid: *miners_paid as u64,
                total_reward: *total_reward,
                pool_fee: *pool_fee,
                paid_to_miners,
                txid: txid.clone(),
                timestamp: *timestamp,
            };
            break;
        }
    }
}

fn truncate_daily(buckets: &mut Vec<DailyBucket>) {
    if buckets.len() > DEFAULT_DAILY_DAYS {
        let start = buckets.len().saturating_sub(DEFAULT_DAILY_DAYS);
        buckets.drain(0..start);
    }
}

fn bump_daily_blocks(buckets: &mut Vec<DailyBucket>, ts: u64) {
    let day = utc_day_string(ts);
    if let Some(last) = buckets.last_mut() {
        if last.date == day {
            last.blocks = last.blocks.saturating_add(1);
            return;
        }
    }
    buckets.push(DailyBucket {
        date: day,
        blocks: 1,
        paid: 0,
    });
}

fn bump_daily_paid(buckets: &mut Vec<DailyBucket>, ts: u64, paid: u64) {
    let day = utc_day_string(ts);
    if let Some(last) = buckets.last_mut() {
        if last.date == day {
            last.paid = last.paid.saturating_add(paid);
            return;
        }
    }
    buckets.push(DailyBucket {
        date: day,
        blocks: 0,
        paid,
    });
}

/// Rebuild blocks_{7d,30d} DAILY series from snapshot.daily_buckets.
/// - Ensures contiguous buckets ending "today" (UTC), filling missing days with 0.
/// - Stores t = day-start unix seconds (UTC), v = blocks as f64.
fn rebuild_block_timeseries_daily_from_daily(snapshot: &mut PoolSnapshot) {
    let today_days = (now_ts() / 86_400) as i64;

    // Map "YYYY-MM-DD" -> blocks
    let mut map: HashMap<String, u64> = HashMap::new();
    for b in snapshot.daily_buckets.iter() {
        map.insert(b.date.clone(), b.blocks);
    }

    snapshot.timeseries.blocks_7d = build_blocks_series_days(&map, today_days, 7);
    snapshot.timeseries.blocks_30d = build_blocks_series_days(&map, today_days, 30);
}

/// Rebuild blocks_1d HOURLY series (24 points) from recent_blocks timestamps.
/// - Buckets are UTC hour-start: (ts / 3600) * 3600
/// - Window is the last 24 hours ending at the current UTC hour bucket (inclusive).
/// - Missing hours filled with 0.
/// - Stores t = hour-start unix seconds (UTC), v = blocks as f64.
fn rebuild_block_timeseries_hourly_1d_from_recent_blocks(
    snapshot: &mut PoolSnapshot,
    recent_blocks: &VecDeque<PoolEvent>,
) {
    let now = now_ts();
    let end_hour = (now / 3_600) * 3_600; // UTC hour bucket start
    let start_hour = end_hour
        .saturating_sub((DEFAULT_BLOCKS_1D_HOURLY_POINTS as u64).saturating_sub(1) * 3_600);

    // Count blocks per hour within [start_hour, end_hour]
    let mut counts: HashMap<u64, u64> = HashMap::new();

    for e in recent_blocks.iter() {
        if let PoolEvent::BlockFound { timestamp, .. } = e {
            let ts = *timestamp;
            if ts < start_hour || ts > (end_hour + 3_600 - 1) {
                continue;
            }
            let hour = (ts / 3_600) * 3_600;
            if hour < start_hour || hour > end_hour {
                continue;
            }
            *counts.entry(hour).or_insert(0) = counts
                .get(&hour)
                .copied()
                .unwrap_or(0)
                .saturating_add(1);
        }
    }

    let mut out = Vec::with_capacity(DEFAULT_BLOCKS_1D_HOURLY_POINTS as usize);
    let mut h = start_hour;
    while h <= end_hour {
        let c = counts.get(&h).copied().unwrap_or(0);
        out.push(TimeSeriesPoint { t: h, v: c as f64 });
        h = h.saturating_add(3_600);
        if out.len() >= DEFAULT_BLOCKS_1D_HOURLY_POINTS as usize {
            break;
        }
    }

    snapshot.timeseries.blocks_1d = out;
}

fn build_blocks_series_days(
    map: &HashMap<String, u64>,
    today_days: i64,
    n_days: i64,
) -> Vec<TimeSeriesPoint> {
    let mut out = Vec::with_capacity(n_days as usize);
    let start_days = today_days - (n_days - 1);
    for d in start_days..=today_days {
        let ts = (d as u64).saturating_mul(86_400);
        let key = utc_day_string(ts);
        let blocks = *map.get(&key).unwrap_or(&0u64);
        out.push(TimeSeriesPoint {
            t: ts,
            v: blocks as f64,
        });
    }
    out
}

fn append_ledger_line(ledger_dir: &Path, ts: u64, event: &PoolEvent) -> anyhow::Result<()> {
    let line = match event {
        PoolEvent::PayoutComplete { .. } => serde_json::to_string(event)?,
        _ => return Ok(()),
    };

    let day = utc_day_string(ts);
    let filename = format!("payout_ledger_{}.jsonl", day);
    let path = ledger_dir.join(filename);

    std::fs::create_dir_all(ledger_dir)?;
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    writeln!(f, "{}", line)?;
    Ok(())
}

fn parse_pool_difficulty_target_from_env() -> Option<[u8; 32]> {
    let s = std::env::var("POOL_DIFFICULTY").ok()?;
    let env: EnvPoolDifficulty = serde_json::from_str(&format!(r#"{{"bytes":{}}}"#, s)).ok()?;
    Some(env.bytes)
}

fn read_pool_difficulty_fixed_hex_from_env() -> Option<String> {
    let bytes = parse_pool_difficulty_target_from_env()?;
    let full = bytes_to_hex_0x(&bytes);
    Some(short_hex(&full))
}

fn compute_fixed_difficulty_u64_from_env() -> Option<u64> {
    let bytes = parse_pool_difficulty_target_from_env()?;

    let target = BigUint::from_bytes_be(&bytes);
    if target == BigUint::from(0u32) {
        return Some(0);
    }

    let max_target = BigUint::from_bytes_be(&[0xFFu8; 32]);
    let diff = max_target / target;

    Some(biguint_to_u64_clamped(&diff))
}

fn biguint_to_u64_clamped(v: &BigUint) -> u64 {
    let bytes = v.to_bytes_be();
    if bytes.len() <= 8 {
        let mut buf = [0u8; 8];
        buf[8 - bytes.len()..].copy_from_slice(&bytes);
        u64::from_be_bytes(buf)
    } else {
        u64::MAX
    }
}

fn bytes_to_hex_0x(bytes: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(2 + 64);
    out.push_str("0x");
    for &b in bytes.iter() {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn short_hex(hex0x: &str) -> String {
    if hex0x.len() >= 2 + 64 {
        let head = &hex0x[..2 + 12];
        let tail = &hex0x[hex0x.len().saturating_sub(10)..];
        return format!("{}…{}", head, tail);
    }
    hex0x.to_string()
}

fn now_ts() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

fn utc_day_string(ts: u64) -> String {
    let days = (ts / 86_400) as i64;
    let (y, m, d) = civil_from_days(days);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = (yoe as i32) + (era as i32) * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (mp + if mp < 10 { 3 } else { -9 }) as u32;
    let y = y + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

// ============================================================================
// File: poolstate.rs
// Location: snap-coin-pool-v2/src/poolstate.rs
// Version: 1.2.1-miner-shares-persist.1
// Created: 2026-02-17T01:00:00Z
// ============================================================================