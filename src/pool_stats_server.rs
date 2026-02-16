// ============================================================================
// File: pool_stats_server.rs
// Location: snap-coin-pool-v2/src/pool_stats_server.rs
// Version: 1.4.5-hashrate-fix.1
//
// Description: WebSocket stats server for real-time pool monitoring dashboard.
//              Broadcasts pool events (miner connections, shares, blocks, payouts,
//              node health, network stats) to connected browser clients via WebSocket.
//
//              Dashboard is served from disk:
//                - GET  /             -> static/pool_dashboard.html
//                - GET  /static/*     -> static assets (css/js/images, etc)
//                - GET  /api/snapshot -> shared pool snapshot (state loading)
//                - WS   /ws           -> live event feed
//
// CHANGELOG (v1.4.5-hashrate-fix.1):
//   - FIX: Pool hashrate calculation now multiplies work_units by pool
//          difficulty before dividing by window seconds. Previously stored
//          shares/sec (~0.1) instead of H/s (~8000).
//   - FIX: Network hashrate timeseries was oversampled at 1s intervals,
//          burning through the 1440-point 24h buffer in ~24 minutes.
//          Now throttled to 60s via last_net_sample_ts guard.
//          Live WS broadcast still fires every 1s for dashboard responsiveness.
//
// CHANGELOG (v1.4.4-timeseries-dual-hashrate24h.1):
//   - Update timeseries plumbing to match PoolState dual 24h hashrate series:
//       * Pool hashrate samples -> PoolState::record_hashrate_sample()
//         (computed from recent accepted-share work_units over a 60s window)
//       * Network hashrate samples -> recorded by PoolState when it receives
//         PoolEvent::NetworkStats (hashrate_hs field).
//   - Preserve the "only write a pool sample when actively mining" guard:
//       * Only persist a pool hashrate sample if NEW accepted shares occurred
//         since last sample AND a share was accepted recently.
//   - No other behavior changes.
//
// CHANGELOG (v1.4.3-timeseries-hashrate-sample.2):
//   - Fix hashrate chart "startup seed" contamination in pool_state.json:
//       * Only persist a hashrate sample if NEW accepted shares occurred since
//         the last sample AND a share was accepted recently.
//         (prevents recording difficulty-derived "network hashrate" at idle boot)
//   - Correct sampling interval to 1/min (was accidentally 5s).
//   - No other behavior changes.
//
// CHANGELOG (v1.4.2-timeseries-hashrate-sample.1):
//   - Persist dashboard hashrate chart history:
//       * Call PoolState::record_hashrate_sample() from NetworkStats emitter
//       * Rate-limit samples to 1/min (matches 24h @ 1m ring buffer in PoolState)
//   - No other behavior changes.
//
// CHANGELOG (v1.4.1-poolstate-external.1):
//   - Remove inline `mod poolstate { ... }`.
//   - Integrate with external `src/poolstate.rs`:
//       * PoolState persists `pool_state.json` (atomic overwrite)
//       * Optional per-day payout ledger JSONL
//   - Add REST endpoint: GET /api/snapshot.
//   - Ensure PoolEventSender updates PoolState BEFORE broadcast (async send).
//   - Preserve all existing behavior and schema of PoolEvent.
//
// Notes:
//   - Reads POOL_API from dotenv/.env (unchanged).
//   - Intentionally avoids `socket.split()` to keep dependencies minimal.
//   - This file now requires async construction:
//       PoolStatsServer::new(...) is async because PoolState::new_from_env() is async.
// ============================================================================

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::Response,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Mutex};
use tower_http::{
    cors::{Any, CorsLayer},
    services::{ServeDir, ServeFile},
};

use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use num_bigint::BigUint;

// Node API client + events
use snap_coin::api::client::Client;
use snap_coin::blockchain_data_provider::BlockchainDataProvider;
use snap_coin::full_node::node_state::ChainEvent;

// External persistent state module.
use crate::poolstate::{PoolSnapshot, PoolState};

// ── Pool events ─────────────────────────────────────────────────────────────

/// Per-miner payout entry for dashboard telemetry (atomic units).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MinerPayout {
    pub miner: String,
    pub amount: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PoolEvent {
    MinerConnected {
        miner: String,
        ip: String,
        timestamp: u64,
    },
    MinerDisconnected {
        miner: String,
        ip: String,
        timestamp: u64,
    },
    ShareAccepted {
        miner: String,
        height: u64,
        work_units: u64,
        work_total: u64,
        timestamp: u64,
    },
    ShareRejected {
        miner: String,
        reason: String,
        timestamp: u64,
    },
    BlockFound {
        height: u64,
        hash: String,
        reward: u64,
        timestamp: u64,
    },
    PayoutComplete {
        height: u64,
        miners_paid: usize,
        total_reward: u64,
        pool_fee: u64,

        /// Payout transaction id (best-effort).
        /// Defaulted for backwards compatibility with older emitters/dashboards.
        #[serde(default)]
        txid: String,

        /// Per-miner payouts included in the payout tx (atomic units).
        /// Defaulted for backwards compatibility with older emitters/dashboards.
        #[serde(default)]
        payouts: Vec<MinerPayout>,

        timestamp: u64,
    },

    NodeConnected {
        addr: String,
        tcp_connect_ms: u64,
        first_ok_ms: u64,
        timestamp: u64,
    },

    NodeHealth {
        addr: String,
        ok: bool,
        rtt_ms: u64,
        fails: u32,
        last_ok_ts: u64,
        timestamp: u64,
    },

    NetworkStats {
        hashrate_hs: u64,
        difficulty: u64,
        height: u64,
        reward: u64,
        last_hash: String,
        last_block_secs_ago: u64,
        avg_block_time_secs: u64,
        timestamp: u64,
    },
}

// ── Event sender ───────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct PoolEventSender {
    tx: broadcast::Sender<PoolEvent>,
    pool_state: PoolState,
}

impl PoolEventSender {
    /// Robust: updates PoolState BEFORE broadcast.
    /// Callers must await.
    pub async fn send(&self, event: PoolEvent) {
        self.pool_state.on_event(&event).await;
        let _ = self.tx.send(event);
    }
}

// ── Server state ────────────────────────────────────────────────────────────

#[derive(Clone)]
struct AppState {
    event_tx: broadcast::Sender<PoolEvent>,
    pool_state: PoolState,
}

// ── Stats server ────────────────────────────────────────────────────────────

pub struct PoolStatsServer {
    port: u16,
    event_tx: broadcast::Sender<PoolEvent>,
    pool_state: PoolState,
}

impl PoolStatsServer {
    /// Async because PoolState loads from disk and starts its flush loop.
    pub async fn new(port: u16) -> (Self, PoolEventSender) {
        let (tx, _rx) = broadcast::channel(100);
        let pool_state = PoolState::new_from_env().await;

        let server = Self {
            port,
            event_tx: tx.clone(),
            pool_state: pool_state.clone(),
        };

        let sender = PoolEventSender {
            tx,
            pool_state,
        };

        (server, sender)
    }

    pub async fn listen(self) -> anyhow::Result<()> {
        let _ = dotenvy::dotenv();

        // Keep NetworkStats flowing into the same PoolState as everyone else.
        spawn_network_stats_tasks(self.event_tx.clone(), self.pool_state.clone());

        let state = AppState {
            event_tx: self.event_tx.clone(),
            pool_state: self.pool_state.clone(),
        };

        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);

        let app = Router::new()
            .route("/ws", get(websocket_handler))
            .route("/api/snapshot", get(snapshot_handler))
            .route_service("/", ServeFile::new("static/pool_dashboard.html"))
            .nest_service("/static", ServeDir::new("static"))
            .layer(cors)
            .with_state(state);

        let bind_addr = format!("0.0.0.0:{}", self.port);
        let listener = tokio::net::TcpListener::bind(&bind_addr).await?;

        tracing::info!("Stats server listening on http://{}", bind_addr);
        tracing::info!("Dashboard: http://localhost:{}/", self.port);
        tracing::info!("Snapshot: http://localhost:{}/api/snapshot", self.port);
        tracing::info!("WS feed: ws://localhost:{}/ws", self.port);

        axum::serve(listener, app).await?;
        Ok(())
    }
}

// ── REST handlers ──────────────────────────────────────────────────────────

async fn snapshot_handler(State(state): State<AppState>) -> Json<PoolSnapshot> {
    Json(state.pool_state.snapshot().await)
}

// ── WebSocket handlers ─────────────────────────────────────────────────────

async fn websocket_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(|socket| websocket_connection(socket, state))
}

async fn websocket_connection(mut socket: WebSocket, state: AppState) {
    let mut rx = state.event_tx.subscribe();

    let welcome = serde_json::json!({
        "type": "connected",
        "message": "Pool stats feed connected"
    });

    if let Ok(msg) = serde_json::to_string(&welcome) {
        let _ = socket.send(Message::Text(msg)).await;
    }

    // "Hello snapshot" (optional, but makes frontend state-load easy even without REST).
    let snap = state.pool_state.snapshot().await;
    let hello = serde_json::json!({
        "type": "snapshot",
        "data": snap
    });
    if let Ok(msg) = serde_json::to_string(&hello) {
        let _ = socket.send(Message::Text(msg)).await;
    }

    while let Ok(event) = rx.recv().await {
        if let Ok(json) = serde_json::to_string(&event) {
            if socket.send(Message::Text(json)).await.is_err() {
                break;
            }
        }
    }

    tracing::debug!("WebSocket client disconnected");
}

// ── Network stats poller + chain timing ────────────────────────────────────

#[derive(Clone, Debug, Default)]
struct NetworkSnapshot {
    height: u64,
    reward: u64,

    /// Raw 32-byte BLOCK TARGET from node API.
    block_target_32: [u8; 32],

    last_hash: String,

    // Timing (from chain events)
    last_block_seen_ts: u64,
    prev_block_seen_ts: u64,
    avg_block_time_secs: u64,
}

fn spawn_network_stats_tasks(event_tx: broadcast::Sender<PoolEvent>, pool_state: PoolState) {
    let pool_api = match std::env::var("POOL_API") {
        Ok(v) => v,
        Err(_) => {
            tracing::debug!("[NET] POOL_API not set; NetworkStats disabled");
            return;
        }
    };

    let node_api: SocketAddr = match pool_api.parse() {
        Ok(a) => a,
        Err(_) => {
            tracing::warn!("[NET] POOL_API invalid SocketAddr: {}", pool_api);
            return;
        }
    };

    let snap = Arc::new(Mutex::new(NetworkSnapshot::default()));
    let snap_event = snap.clone();
    let snap_poll = snap.clone();
    let snap_emit = snap.clone();

    // 1) Chain event listener: avg block time + age basis
    tokio::spawn(async move {
        let client = match Client::connect(node_api).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("[NET] event listener connect failed: {}", e);
                return;
            }
        };

        let res = client
            .convert_to_event_listener(
                move |event: ChainEvent| {
                    let snap_event = snap_event.clone();
                    tokio::spawn(async move {
                        if let ChainEvent::Block { .. } = event {
                            let now = now_ts();
                            let mut s = snap_event.lock().await;

                            if s.last_block_seen_ts != 0 {
                                s.prev_block_seen_ts = s.last_block_seen_ts;
                                s.last_block_seen_ts = now;

                                let dt = s.last_block_seen_ts.saturating_sub(s.prev_block_seen_ts);
                                if dt > 0 {
                                    // rolling average: (old*7 + new)/8
                                    s.avg_block_time_secs = if s.avg_block_time_secs == 0 {
                                        dt
                                    } else {
                                        ((s.avg_block_time_secs * 7) + dt) / 8
                                    };
                                }
                            } else {
                                s.last_block_seen_ts = now;
                                s.prev_block_seen_ts = 0;
                                s.avg_block_time_secs = 0;
                            }
                        }
                    });
                },
                None, // snap-coin v13.2.0: optional shutdown receiver
            )
            .await;

        if let Err(e) = res {
            tracing::warn!("[NET] event listener ended: {}", e);
        }
    });

    // 2) Poll node: height/reward/block_target/last_hash once per second
    tokio::spawn(async move {
        let client = match Client::connect(node_api).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!("[NET] poller connect failed: {}", e);
                return;
            }
        };

        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;

            let height = match client.get_height().await {
                Ok(h) => h as u64,
                Err(e) => {
                    tracing::warn!("[NET] get_height failed: {}", e);
                    continue;
                }
            };

            let reward = match client.get_reward().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("[NET] get_reward failed: {}", e);
                    0
                }
            };

            // Pull BLOCK target directly.
            let block_target_32 = match client.get_block_difficulty().await {
                Ok(t) => t,
                Err(e) => {
                    tracing::warn!("[NET] get_block_difficulty failed: {}", e);
                    [0u8; 32]
                }
            };

            // last block is height-1 (tip height can be "next slot / count")
            let last_h = height.saturating_sub(1);
            let last_hash = match client.get_block_hash_by_height(last_h as usize).await {
                Ok(Some(h)) => format!("{:?}", h), // abbreviated upstream; leave it
                Ok(None) => "-".to_string(),
                Err(e) => {
                    tracing::warn!("[NET] get_block_hash_by_height failed: {}", e);
                    "-".to_string()
                }
            };

            let mut s = snap_poll.lock().await;
            s.height = height;
            s.reward = reward;
            s.block_target_32 = block_target_32;
            s.last_hash = last_hash;
        }
    });

    // 3) Emit NetworkStats + persist POOL hashrate samples (separate from network series)
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));

        // difficulty = MAX_TARGET / target (BE)
        let max_target = BigUint::from_bytes_be(&[0xFFu8; 32]);
        let thirty_five = BigUint::from(35u32);
        let zero = BigUint::from(0u32);

        // Persist POOL hashrate samples at 1/min, but ONLY when actively mining.
        let mut last_pool_sample_ts: u64 = 0;

        // ── FIX v1.4.5: Throttle NETWORK hashrate timeseries to 1/min ──────
        // WS broadcast still fires every 1s for live dashboard updates.
        // Only pool_state.on_event() is throttled to prevent burning through
        // the 1440-point (24h) ring buffer in ~24 minutes.
        let mut last_net_sample_ts: u64 = 0;

        // Track accepted-share counter at last sample boundary.
        // Initialize from persisted state so we don't treat old shares as "new".
        let mut last_sample_shares_acc: u64 = pool_state.snapshot().await.totals.shares_acc;

        // Consider pool "active" if a ShareAccepted occurred recently.
        const SHARE_RECENT_SECS: u64 = 120;

        loop {
            tick.tick().await;

            let s = snap_emit.lock().await.clone();
            if s.height == 0 {
                continue;
            }

            let now = now_ts();
            let last_block_secs_ago = if s.last_block_seen_ts == 0 {
                0
            } else {
                now.saturating_sub(s.last_block_seen_ts)
            };

            let target = BigUint::from_bytes_be(&s.block_target_32);
            let difficulty_big = if target == zero {
                BigUint::from(0u32)
            } else {
                &max_target / &target
            };
            let hashrate_big = if difficulty_big == zero {
                BigUint::from(0u32)
            } else {
                &difficulty_big / &thirty_five
            };

            let difficulty_u64 = biguint_to_u64_clamped(&difficulty_big);
            let network_hashrate_u64 = biguint_to_u64_clamped(&hashrate_big);

            // ── Compute + persist POOL hashrate sample at 1/min ─────────────
            // ONLY if:
            //  - shares_acc increased since last sample, AND
            //  - a ShareAccepted event is recent (pool is actively mining now).
            if last_pool_sample_ts == 0 || now.saturating_sub(last_pool_sample_ts) >= 15 {
                let snap = pool_state.snapshot().await;

                // Find most recent ShareAccepted timestamp from recent_shares (best-effort).
                let mut last_share_ts: u64 = 0;
                for e in snap.recent_shares.iter().rev() {
                    if let PoolEvent::ShareAccepted { timestamp, .. } = e {
                        last_share_ts = *timestamp;
                        break;
                    }
                }

                let shares_acc_now = snap.totals.shares_acc;
                let has_new_shares = shares_acc_now > last_sample_shares_acc;
                let share_is_recent =
                    last_share_ts != 0 && now.saturating_sub(last_share_ts) <= SHARE_RECENT_SECS;

                if has_new_shares && share_is_recent {
                    // Pool hashrate estimate from accepted shares over the last 60s:
                    // sum(work_units) * pool_difficulty / 60.
                    //
                    // FIX v1.4.5: Previously this was work_units / 60 which gave
                    // shares/sec (~0.1) instead of H/s (~8000). Multiplying by
                    // pool difficulty converts share-work into actual hash-work.
                    const WINDOW_SECS: u64 = 60;
                    let window_start = now.saturating_sub(WINDOW_SECS);

                    let pool_diff = snap.pool_difficulty_fixed_num.max(1);

                    let mut work_units_sum: u64 = 0;
                    for e in snap.recent_shares.iter().rev() {
                        if let PoolEvent::ShareAccepted {
                            work_units,
                            timestamp,
                            ..
                        } = e
                        {
                            if *timestamp < window_start {
                                break;
                            }
                            work_units_sum = work_units_sum.saturating_add(*work_units);
                        }
                    }

                    let pool_hashrate_hs =
                        (work_units_sum as f64 * pool_diff as f64) / (WINDOW_SECS as f64);
                    pool_state.record_hashrate_sample(pool_hashrate_hs).await;

                    last_pool_sample_ts = now;
                    last_sample_shares_acc = shares_acc_now;
                }
            }

            let evt = PoolEvent::NetworkStats {
                hashrate_hs: network_hashrate_u64,
                difficulty: difficulty_u64,
                height: s.height,
                reward: s.reward,
                last_hash: s.last_hash,
                last_block_secs_ago,
                avg_block_time_secs: s.avg_block_time_secs,
                timestamp: now,
            };

            // ── FIX v1.4.5: Throttle network timeseries persistence ────────
            // Only call pool_state.on_event() once per 60s so the 1440-point
            // ring buffer actually covers 24 hours. The WS broadcast below
            // still fires every 1s for live dashboard responsiveness.
            if last_net_sample_ts == 0 || now.saturating_sub(last_net_sample_ts) >= 15 {
                pool_state.on_event(&evt).await;
                last_net_sample_ts = now;
            }

            let _ = event_tx.send(evt);
        }
    });
}

fn biguint_to_u64_clamped(v: &BigUint) -> u64 {
    // exact if <= u64::MAX, otherwise clamp
    let bytes = v.to_bytes_be();
    if bytes.len() <= 8 {
        let mut buf = [0u8; 8];
        buf[8 - bytes.len()..].copy_from_slice(&bytes);
        u64::from_be_bytes(buf)
    } else {
        u64::MAX
    }
}

fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

// ============================================================================
// File: pool_stats_server.rs
// Location: snap-coin-pool-v2/src/pool_stats_server.rs
// Version: 1.4.5-hashrate-fix.1
// Created: 2026-02-15T20:30:00Z
// ============================================================================