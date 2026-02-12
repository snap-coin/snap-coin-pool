// ============================================================================
// File: poolstate.rs
// Location: snap-coin-pool-v2/src/poolstate.rs
// Version: 1.0.0-snapshot-ledger.1
//
// Description: Server-side shared pool state (snapshot hydration) + lightweight
//              disk persistence (no DB).
//
//              Provides:
//                - Overwritten JSON snapshot file: pool_state.json (atomic write)
//                - Optional append-only payout ledger: payout_ledger_YYYY-MM-DD.jsonl
//
//              Intended integration points (from pool_stats_server.rs):
//                - pool_state.on_event(&PoolEvent)  (for EVERY emitted event)
//                - GET /api/snapshot -> pool_state.snapshot()
//
// Notes:
//   - This module intentionally stores ONLY what the dashboard needs.
//   - Recent events are bounded (default 120).
//   - Daily buckets are bounded (default 30 days).
//   - Uses an internal UTC day formatter (civil_from_days) to avoid adding chrono.
// ============================================================================

use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

/// Import PoolEvent from your stats server module.
///
/// IMPORTANT:
/// This assumes your crate has `mod pool_stats_server;` and the file you pasted
/// lives at `src/pool_stats_server.rs` (module name `pool_stats_server`).
use crate::pool_stats_server::PoolEvent;

const DEFAULT_STATE_FILE: &str = "pool_state.json";
const DEFAULT_LEDGER_DIR: &str = ".";
const DEFAULT_RECENT_EVENTS: usize = 120;
const DEFAULT_DAILY_DAYS: usize = 30;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct DailyBucket {
    pub date: String, // "YYYY-MM-DD" (UTC)
    pub blocks: u64,
    pub paid: u64, // atomic units (sum paid to miners)
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
pub struct Network {
    pub height: u64,
    pub reward: u64,
    pub difficulty: u64,
    pub hashrate_hs: u64,
    pub last_hash: String,
    pub last_block_secs_ago: u64,
    pub avg_block_time_secs: u64,
    pub timestamp: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PoolSnapshot {
    pub generated_at: u64,
    pub totals: Totals,
    pub network: Network,
    pub last_block: LastBlock,
    pub daily_buckets: Vec<DailyBucket>,
    pub recent_events: Vec<PoolEvent>,
}

#[derive(Debug)]
struct Inner {
    snapshot: PoolSnapshot,
    recent_events: VecDeque<PoolEvent>,
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
    /// Create PoolState, loading an existing snapshot file if present.
    ///
    /// Env vars:
    ///   - POOL_STATE_FILE   (default "pool_state.json")
    ///   - POOL_LEDGER_DIR   (default ".")
    ///   - POOL_LEDGER_ENABLE ("1" default on; set "0" to disable)
    pub async fn new_from_env() -> Self {
        let state_file = std::env::var("POOL_STATE_FILE").unwrap_or_else(|_| DEFAULT_STATE_FILE.to_string());
        let ledger_dir = std::env::var("POOL_LEDGER_DIR").unwrap_or_else(|_| DEFAULT_LEDGER_DIR.to_string());
        let ledger_enable = std::env::var("POOL_LEDGER_ENABLE")
            .ok()
            .map(|v| v != "0")
            .unwrap_or(true);

        let state_path = PathBuf::from(state_file);

        // Load existing snapshot file if present.
        let mut snapshot = PoolSnapshot::default();
        if let Ok(bytes) = tokio::fs::read(&state_path).await {
            if let Ok(loaded) = serde_json::from_slice::<PoolSnapshot>(&bytes) {
                snapshot = loaded;
            }
        }

        // Ensure generated_at updates at startup.
        snapshot.generated_at = now_ts();

        // Seed ring buffer from loaded snapshot.
        let mut recent_events = VecDeque::with_capacity(DEFAULT_RECENT_EVENTS);
        for e in snapshot.recent_events.iter().cloned().take(DEFAULT_RECENT_EVENTS) {
            recent_events.push_back(e);
        }

        let inner = Inner {
            snapshot,
            recent_events,
            dirty: false,
            state_file: state_path,
            ledger_enable,
            ledger_dir: PathBuf::from(ledger_dir),
        };

        let this = Self {
            inner: Arc::new(Mutex::new(inner)),
        };

        // Debounced flush loop (writes pool_state.json at most once per second).
        this.spawn_flush_task();

        this
    }

    /// Get a clone of the current snapshot (for REST /api/snapshot).
    pub async fn snapshot(&self) -> PoolSnapshot {
        let mut g = self.inner.lock().await;

        // Export ring to snapshot
        g.snapshot.generated_at = now_ts();
        g.snapshot.recent_events = g.recent_events.iter().cloned().collect();

        g.snapshot.clone()
    }

    /// Update state for a given event.
    /// Call this for *every* PoolEvent you broadcast.
    pub async fn on_event(&self, event: &PoolEvent) {
        let mut g = self.inner.lock().await;

        // Maintain recent events ring.
        g.recent_events.push_back(event.clone());
        while g.recent_events.len() > DEFAULT_RECENT_EVENTS {
            g.recent_events.pop_front();
        }

        // Update aggregates.
        match event {
            PoolEvent::ShareAccepted { .. } => {
                g.snapshot.totals.shares_acc = g.snapshot.totals.shares_acc.saturating_add(1);
            }
            PoolEvent::ShareRejected { .. } => {
                g.snapshot.totals.shares_rej = g.snapshot.totals.shares_rej.saturating_add(1);
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
            }
            PoolEvent::PayoutComplete {
                payouts,
                total_reward,
                pool_fee,
                timestamp,
                ..
            } => {
                // Best-effort paid_to_miners:
                // - If payouts[] present, sum it (authoritative miner-paid amount)
                // - Else fallback to total_reward - pool_fee (older events).
                let paid_to_miners = if !payouts.is_empty() {
                    payouts.iter().fold(0u64, |acc, p| acc.saturating_add(p.amount))
                } else {
                    total_reward.saturating_sub(*pool_fee)
                };

                g.snapshot.totals.total_paid_to_miners =
                    g.snapshot.totals.total_paid_to_miners.saturating_add(paid_to_miners);

                bump_daily_paid(&mut g.snapshot.daily_buckets, *timestamp, paid_to_miners);
                truncate_daily(&mut g.snapshot.daily_buckets);

                // Append ledger line (append-only), best-effort.
                if g.ledger_enable {
                    let _ = append_ledger_line(&g.ledger_dir, *timestamp, event);
                }
            }
            PoolEvent::NetworkStats {
                hashrate_hs,
                difficulty,
                height,
                reward,
                last_hash,
                last_block_secs_ago,
                avg_block_time_secs,
                timestamp,
            } => {
                g.snapshot.network.hashrate_hs = *hashrate_hs;
                g.snapshot.network.difficulty = *difficulty;
                g.snapshot.network.height = *height;
                g.snapshot.network.reward = *reward;
                g.snapshot.network.last_hash = last_hash.clone();
                g.snapshot.network.last_block_secs_ago = *last_block_secs_ago;
                g.snapshot.network.avg_block_time_secs = *avg_block_time_secs;
                g.snapshot.network.timestamp = *timestamp;
            }
            _ => {}
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

                // Refresh exported recent_events + generated_at before writing.
                g.snapshot.generated_at = now_ts();
                g.snapshot.recent_events = g.recent_events.iter().cloned().collect();

                let path = g.state_file.clone();
                let json = match serde_json::to_vec_pretty(&g.snapshot) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                // Atomic write: write tmp then rename.
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

// ── helpers ────────────────────────────────────────────────────────────────

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

fn append_ledger_line(ledger_dir: &Path, ts: u64, event: &PoolEvent) -> anyhow::Result<()> {
    // Only log payout events.
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

fn now_ts() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

/// Convert a unix-seconds timestamp to UTC day string "YYYY-MM-DD".
///
/// Uses a small civil date conversion (no chrono dependency).
fn utc_day_string(ts: u64) -> String {
    let days = (ts / 86_400) as i64;
    let (y, m, d) = civil_from_days(days);
    format!("{:04}-{:02}-{:02}", y, m, d)
}

// Howard Hinnant's civil_from_days (commonly used; public-domain style).
fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = (yoe as i32) + (era as i32) * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (mp + if mp < 10 { 3 } else { -9 }) as u32; // [1, 12]
    let y = y + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

// ============================================================================
// File: poolstate.rs
// Location: snap-coin-pool-v2/src/poolstate.rs
// Version: 1.0.0-snapshot-ledger.1
// Updated: 2026-02-11
// ============================================================================
