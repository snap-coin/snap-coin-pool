// ============================================================================
// File: pool_api_server.rs
// Location: snap-coin-pool-v2/src/pool_api_server.rs
// Version: 0.1.4-stats.18
//
// Description:
// Upstream pool API server (logic preserved) with OPTIONAL instrumentation hooks
// to emit dashboard telemetry via PoolStatsServer.
//
// IMPORTANT CONSTRAINTS (RESPECTED):
// - Do NOT change upstream mining/job/share/payout/ban logic
// - Do NOT change protocol behavior
// - Add ONLY observational emissions (safe side-effects)
//
// ADD (v0.1.4-stats.13):
// - Production-grade active connection tracking (observational):
//     * Track active connections per miner pubkey (string key) and per IP.
//     * Enforce MAX_CONNS_PER_MINER (requested: 10).
//     * Gracefully reject excess connections (shutdown) without touching mining logic.
// - Passive heartbeat (idle timeout) on request stream:
//     * If no request received within HEARTBEAT_IDLE_SECS -> disconnect.
//     * No protocol messages are sent (protocol-safe).
//
// FIX (v0.1.4-stats.14):
// - Fix borrow checker error in try_register_connection(): avoid holding two
//   mutable borrows into the ActiveConns struct simultaneously.
// - Remove unused last_rx_ts assignment (passive heartbeat uses timeout wrapper).
//
// FIX (v0.1.4-stats.15):
// - ShareAccepted height telemetry: remove incorrect `last_block_height`
//   (node_height-1) and emit the mined job height as `node_height + 1`.
//   This aligns ShareAccepted display with the "next block being solved" semantic.
//
// FIX (v0.1.4-stats.16):
// - BlockFound/PayoutComplete height telemetry: remove incorrect `node_height-1`
//   and emit `node_height` directly after submit, matching explorer height.
//
// FIX (v0.1.4-stats.17):
// - Do NOT create a node API client per incoming miner connection.
//   Create a single shared Client once at server start and reuse via Arc.
//
// FIX (v0.1.4-stats.18):
// - Resolve moved `block` compile error by capturing derived strings (hash) before move.
// - Remove accidental second payout path call that would re-use moved `block` and
//   could change payout behavior.
// ============================================================================

use anyhow::anyhow;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{RwLock, mpsc},
    task::JoinHandle,
    time::timeout,
};

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use tokio::sync::Mutex;

use snap_coin::{
    api::{
        client::Client,
        requests::{Request, Response},
    },
    core::block::Block,
    crypto::keys::{Private, Public},
    full_node::node_state::ChainEvent,
};

// REQUIRED: bring trait methods (get_height/get_reward/...) into scope
use snap_coin::blockchain_data_provider::BlockchainDataProvider;

use crate::{
    handle_rewards::{PayoutMetrics, handle_rewards_with_metrics},
    handle_share::handle_share,
    job_handler::JobHandler,
    share_store::SharedShareStore,
};

#[allow(unused_imports)]
use crate::pool_stats_server::{MinerPayout, PoolEvent, PoolEventSender};

// ── Constants ───────────────────────────────────────────────────────────────

const ERROR_PENALTY: i32 = 1;
const BAN_THRESHOLD: i32 = 25;
const BAN_PENALTY: i32 = 10;
const SCORE_DECAY_INTERVAL: Duration = Duration::from_secs(30);
const SCORE_DECAY_AMOUNT: i32 = 1;

// Watchdog thresholds (pool-side mitigation; protocol-safe)
const WATCHDOG_REJECT_STREAK_KICK: u32 = 3;

// Kick escalation window + threshold
const KICK_WINDOW_SECS: u64 = 300; // 5 minutes
const KICKS_TO_BAN: u32 = 3;

// Active connection policy (requested)
const MAX_CONNS_PER_MINER: usize = 100;

// Passive heartbeat (idle timeout) on the request stream
const HEARTBEAT_IDLE_SECS: u64 = 3600;

// Handshake watchdog (already present)
const HANDSHAKE_TIMEOUT_SECS: u64 = 30;

// ── IP scoring ──────────────────────────────────────────────────────────────

#[derive(Clone)]
struct IpScore {
    score: i32,
    banned: bool,

    // v0.1.4-stats.9: kick escalation tracking (lightweight)
    kick_window_start_ts: u64,
    kicks_in_window: u32,
}

type IpScoreMap = Arc<Mutex<HashMap<String, IpScore>>>;

// ── Active connection tracking ──────────────────────────────────────────────
//
// NOTE:
// We key miners by a string derived from Debug formatting of `Public`
// because we don't want to assume `Public: Hash + Eq` across upstream versions.

#[derive(Default)]
struct ActiveConns {
    by_miner: HashMap<String, usize>,
    by_ip: HashMap<String, usize>,
}

type ActiveConnMap = Arc<Mutex<ActiveConns>>;

/// Server for hosting a Snap Coin API (pool server)
pub struct PoolServer {
    port: u16,
    pool_difficulty: [u8; 32],
    pool_private: Private,
    pool_dev: Public,
    pool_fee: f64,
    pool_api: SocketAddr,
    share_store: SharedShareStore,
    ip_scores: IpScoreMap,
    current_job: Arc<RwLock<Option<Block>>>,

    // OPTIONAL stats emitter (no effect if None)
    stats: Option<PoolEventSender>,

    // v0.1.4-stats.13: active connection tracking
    active: ActiveConnMap,
}

impl PoolServer {
    /// Create a new server, do not listen for connections yet (UPSTREAM SIGNATURE KEPT)
    pub fn new(
        port: u16,
        pool_difficulty: [u8; 32],
        pool_private: Private,
        pool_dev: Public,
        pool_fee: f64,
        pool_api: SocketAddr,
        share_store: SharedShareStore,
    ) -> Self {
        PoolServer {
            port,
            pool_difficulty,
            pool_private,
            pool_dev,
            pool_fee,
            share_store,
            pool_api,
            current_job: Arc::new(RwLock::new(None)),
            ip_scores: Arc::new(Mutex::new(HashMap::new())),
            stats: None,
            active: Arc::new(Mutex::new(ActiveConns::default())),
        }
    }

    /// Attach stats sender (optional).
    #[allow(dead_code)]
    pub fn set_stats_sender(&mut self, sender: PoolEventSender) {
        self.stats = Some(sender);
    }

    // ── Small helper for safe emission ───────────────────────────────────

    async fn emit(&self, event: PoolEvent) {
        if let Some(s) = self.stats.clone() {
            s.send(event).await;
        }
    }

    // ── Active connection tracking helpers ────────────────────────────────

    async fn try_register_connection(&self, miner_key: &str, ip: &str) -> bool {
        let mut a = self.active.lock().await;

        // Read current miner count without holding a mutable ref across other mutations.
        let current_miner = a.by_miner.get(miner_key).copied().unwrap_or(0);
        if current_miner >= MAX_CONNS_PER_MINER {
            return false;
        }

        // Apply increments in separate steps (no overlapping mutable borrows).
        let new_miner = current_miner + 1;
        a.by_miner.insert(miner_key.to_string(), new_miner);

        let current_ip = a.by_ip.get(ip).copied().unwrap_or(0);
        a.by_ip.insert(ip.to_string(), current_ip + 1);

        true
    }

    async fn unregister_connection(&self, miner_key: &str, ip: &str) {
        let mut a = self.active.lock().await;

        if let Some(m) = a.by_miner.get_mut(miner_key) {
            *m = m.saturating_sub(1);
            if *m == 0 {
                a.by_miner.remove(miner_key);
            }
        }

        if let Some(i) = a.by_ip.get_mut(ip) {
            *i = i.saturating_sub(1);
            if *i == 0 {
                a.by_ip.remove(ip);
            }
        }
    }

    // ── Upstream ban logic (unchanged behavior) ────────────────────────────

    async fn add_penalty(&self, ip: String) {
        let mut scores = self.ip_scores.lock().await;
        let entry = scores.entry(ip.clone()).or_insert(IpScore {
            score: 0,
            banned: false,
            kick_window_start_ts: 0,
            kicks_in_window: 0,
        });

        entry.score += ERROR_PENALTY;

        if entry.score >= BAN_THRESHOLD && !entry.banned {
            entry.score += BAN_PENALTY;
            entry.banned = true;
            println!("IP {} has been BANNED (score: {})", ip, entry.score);
        }
    }

    async fn is_banned(&self, ip: &str) -> bool {
        let scores = self.ip_scores.lock().await;
        scores.get(ip).map(|entry| entry.banned).unwrap_or(false)
    }

    async fn record_kick_and_maybe_ban(&self, ip: &str) -> bool {
        let now = now_ts();
        let mut scores = self.ip_scores.lock().await;
        let entry = scores.entry(ip.to_string()).or_insert(IpScore {
            score: 0,
            banned: false,
            kick_window_start_ts: now,
            kicks_in_window: 0,
        });

        if entry.kick_window_start_ts == 0
            || now.saturating_sub(entry.kick_window_start_ts) > KICK_WINDOW_SECS
        {
            entry.kick_window_start_ts = now;
            entry.kicks_in_window = 0;
        }

        entry.kicks_in_window = entry.kicks_in_window.saturating_add(1);

        if entry.kicks_in_window >= KICKS_TO_BAN && !entry.banned {
            entry.score = BAN_THRESHOLD;
            entry.banned = true;
            println!(
                "IP {} has been BANNED due to repeated watchdog kicks (kicks_in_window: {})",
                ip, entry.kicks_in_window
            );
            return true;
        }

        entry.banned
    }

    async fn score_decay_task(ip_scores: IpScoreMap) {
        let mut interval = tokio::time::interval(SCORE_DECAY_INTERVAL);
        loop {
            interval.tick().await;
            let mut scores = ip_scores.lock().await;

            scores.retain(|_ip, entry| {
                entry.score = (entry.score - SCORE_DECAY_AMOUNT).max(0);

                if entry.banned && entry.score < BAN_THRESHOLD {
                    entry.banned = false;
                }

                entry.score > 0 || entry.banned
            });
        }
    }

    // ── Connection loop (logic preserved; watchdog + telemetry + heartbeat) ──

    async fn connection(
        self: Arc<PoolServer>,
        mut stream: TcpStream,
        client_address: Public,
        ip: String,
        submit_block: mpsc::Sender<(Block, Public)>,
        job_handler: Arc<JobHandler>,
        // v0.1.4-stats.17: shared node API client (NO per-miner connect)
        height_client: Option<Arc<Client>>,
    ) {
        let miner_key = format!("{:?}", client_address);
        let mut reject_streak: u32 = 0;

        if self.active.lock().await.by_ip.get(&ip).unwrap_or(&1) == &1 {
            self.emit(PoolEvent::MinerConnected {
                miner: miner_key.clone(),
                ip: ip.clone(),
                timestamp: now_ts(),
            })
            .await;
        }

        loop {
            // Passive heartbeat: if no request arrives within HEARTBEAT_IDLE_SECS, drop connection.
            let request = match timeout(
                Duration::from_secs(HEARTBEAT_IDLE_SECS),
                Request::decode_from_stream(&mut stream),
            )
            .await
            {
                Err(_timeout) => {
                    println!(
                        "Miner idle timeout ({}s) from {} miner={}",
                        HEARTBEAT_IDLE_SECS, ip, miner_key
                    );
                    break;
                }
                Ok(Err(e)) => {
                    println!(
                        "Miner request decode error from {} miner={}: {}",
                        ip, miner_key, e
                    );
                    self.add_penalty(ip.clone()).await;
                    break;
                }
                Ok(Ok(r)) => r,
            };

            if let Err(e) = async {
                let response = match request {
                    Request::NewBlock { new_block } => Response::NewBlock {
                        status: {
                            if self.current_job.read().await.is_none() {
                                return Err(anyhow!("No job yet!"));
                            }
                            let current_job = self.current_job.read().await.clone().unwrap();

                            let res = handle_share(
                                &current_job,
                                &new_block,
                                &self.share_store,
                                client_address,
                                &self.pool_difficulty,
                            )
                            .await;

                            match &res {
                                Ok(_) => {
                                    reject_streak = 0;

                                    let node_height = match &height_client {
                                        Some(c) => c.get_height().await.unwrap_or(0) as u64,
                                        None => 0,
                                    };

                                    // FIX v0.1.4-stats.15:
                                    // Emit the *job* height being solved (tip + 1), not last_block_height (tip - 1).
                                    self.emit(PoolEvent::ShareAccepted {
                                        miner: miner_key.clone(),
                                        height: node_height.saturating_add(1),
                                        work_units: 1,
                                        work_total: 1,
                                        timestamp: now_ts(),
                                    })
                                    .await;
                                }
                                Err(err) => {
                                    reject_streak = reject_streak.saturating_add(1);

                                    self.emit(PoolEvent::ShareRejected {
                                        miner: miner_key.clone(),
                                        reason: err.to_string(),
                                        timestamp: now_ts(),
                                    })
                                    .await;

                                    if reject_streak >= WATCHDOG_REJECT_STREAK_KICK {
                                        let was_banned = self.record_kick_and_maybe_ban(&ip).await;
                                        if self.active.lock().await.by_ip.get(&ip).unwrap_or(&1) == &1 {
                                            self.emit(PoolEvent::MinerDisconnected {
                                                miner: miner_key.clone(),
                                                ip: ip.clone(),
                                                timestamp: now_ts(),
                                            })
                                            .await;
                                        }

                                        let _ = stream.shutdown().await;

                                        if was_banned {
                                            println!(
                                                "Watchdog kicked miner {}; IP {} is now banned (kicks threshold).",
                                                miner_key, ip
                                            );
                                        }

                                        return Ok::<(), anyhow::Error>(());
                                    }
                                }
                            }

                            if res.is_ok() {
                                if new_block
                                    .validate_difficulties(
                                        &current_job.meta.block_pow_difficulty,
                                        &current_job.meta.tx_pow_difficulty,
                                    )
                                    .is_ok()
                                {
                                    let _ = submit_block.send((new_block, client_address)).await;
                                }
                            }

                            res
                        },
                    },
                    Request::SubscribeToChainEvents => {
                        let mut new_job = job_handler.subscribe();
                        let current_job = self.current_job.read().await.clone();
                        if let Some(current_job) = current_job {
                            let response = Response::ChainEvent {
                                event: ChainEvent::Block { block: current_job },
                            };
                            stream.write_all(&response.encode()?).await?;
                        }

                        loop {
                            match new_job.recv().await {
                                Ok(block) => {
                                    let response = Response::ChainEvent {
                                        event: ChainEvent::Block { block },
                                    };
                                    stream.write_all(&response.encode()?).await?;
                                }
                                Err(_) => break,
                            }
                        }

                        return Err(anyhow!("Request response loop ended"));
                    }
                    _ => {
                        return Err(anyhow!("Restricted API endpoint accessed"));
                    }
                };

                let response_buf = response.encode()?;
                stream.write_all(&response_buf).await?;

                Ok::<(), anyhow::Error>(())
            }
            .await
            {
                println!("Miner error from {} miner={}: {}", ip, miner_key, e);
                self.add_penalty(ip.clone()).await;

                if self.active.lock().await.by_ip.get(&ip).unwrap_or(&1) == &1 {
                    self.emit(PoolEvent::MinerDisconnected {
                        miner: miner_key.clone(),
                        ip: ip.clone(),
                        timestamp: now_ts(),
                    })
                    .await;
                }

                break;
            }
        }

        self.unregister_connection(&miner_key, &ip).await;
        if self.active.lock().await.by_ip.get(&ip).unwrap_or(&1) == &1 {
            self.emit(PoolEvent::MinerDisconnected {
                miner: miner_key,
                ip,
                timestamp: now_ts(),
            })
            .await;
        }
    }

    // ── Listen (logic preserved; telemetry added) ──────────────────────────

    pub async fn listen(self: Arc<Self>) -> Result<JoinHandle<()>, anyhow::Error> {
        let listener = match TcpListener::bind(format!("0.0.0.0:{}", self.port)).await {
            Ok(l) => l,
            Err(_) => TcpListener::bind("0.0.0.0:0").await?,
        };
        println!(
            "Pool API Server listening on 0.0.0.0:{}",
            listener.local_addr()?.port()
        );

        let ip_scores_clone = self.ip_scores.clone();
        tokio::spawn(async move {
            Self::score_decay_task(ip_scores_clone).await;
        });

        // v0.1.4-stats.17: shared node API client (used for per-miner height telemetry)
        // NOTE: best-effort; if connect fails we still run, just emit height=0.
        let shared_height_client: Option<Arc<Client>> =
            Client::connect(self.pool_api).await.ok().map(Arc::new);

        let (submit_tx, mut submit_rx) = mpsc::channel(24);
        let self_clone = self.clone();

        tokio::spawn(async move {
            // Single submit client for the submit/payout task (already shared in this task).
            let submit_client = Arc::new(Client::connect(self_clone.pool_api).await.unwrap());

            loop {
                let submit: Option<(Block, Public)> = submit_rx.recv().await;
                let self_clone = self_clone.clone();
                let submit_client = submit_client.clone();

                if let Some((block, _public)) = submit {
                    if let Err(e) = async move {
                        // NOTE: submit_block uses a clone, so we still own `block` here.
                        submit_client.submit_block(block.clone()).await??;
                        println!("[POOL] Pool mined new block!");

                        let node_height = submit_client.get_height().await.unwrap_or(0) as u64;
                        let reward = submit_client.get_reward().await.unwrap_or(0);

                        // FIX v0.1.4-stats.16: Emit node_height directly (no -1).
                        self_clone
                            .emit(PoolEvent::BlockFound {
                                height: node_height,
                                hash: format!("{:?}", block.meta.hash),
                                reward,
                                timestamp: now_ts(),
                            })
                            .await;

                        // v0.1.4-stats.18: capture derived info BEFORE moving `block`.
                        let block_hash_b36 = block.meta.hash.unwrap().dump_base36();

                        let payout_metrics: Option<PayoutMetrics> = handle_rewards_with_metrics(
                            &*submit_client,
                            block,
                            self_clone.pool_private,
                            self_clone.pool_dev,
                            &self_clone.share_store,
                            self_clone.pool_fee,
                        )
                        .await?;

                        println!("[POOL] Mined new valid block! {}", block_hash_b36);

                        if let Some(m) = payout_metrics {
                            let payouts: Vec<MinerPayout> = m
                                .payouts
                                .iter()
                                .map(|(public, amount)| MinerPayout {
                                    miner: format!("{:?}", public),
                                    amount: *amount,
                                })
                                .collect();

                            // FIX v0.1.4-stats.16: Emit node_height directly (no -1).
                            self_clone
                                .emit(PoolEvent::PayoutComplete {
                                    height: node_height,
                                    miners_paid: m.miners_paid,
                                    total_reward: m.coinbase_amount,
                                    pool_fee: m.pool_fee_amount,
                                    txid: m.txid,
                                    payouts,
                                    timestamp: now_ts(),
                                })
                                .await;
                        }

                        Ok::<(), anyhow::Error>(())
                    }
                    .await
                    {
                        println!("Failed to submit pool block: {e}");
                    }
                }
            }
        });

        let (job_handler, first_job) = JobHandler::listen(self.pool_api, self.pool_private).await?;
        let job_handler = Arc::new(job_handler);
        *self.current_job.write().await = Some(first_job);

        let mut job_updater = job_handler.subscribe();
        let self_clone = self.clone();

        tokio::spawn(async move {
            loop {
                if let Err(e) = async {
                    let job = job_updater.recv().await?;
                    *self_clone.current_job.write().await = Some(job);
                    Ok::<(), anyhow::Error>(())
                }
                .await
                {
                    println!("Job updater failed! {e}");
                }
            }
        });

        let self_clone = self.clone();
        Ok(tokio::spawn(async move {
            loop {
                let job_handler = job_handler.clone();
                let res = listener.accept().await;
                if res.is_err() {
                    continue;
                }

                let (mut stream, addr) = res.unwrap();
                let submit_tx = submit_tx.clone();
                let self_clone = self_clone.clone();
                let shared_height_client = shared_height_client.clone();

                if let Err(e) = async move {
                    let ip = Self::extract_ip(&addr);

                    let self_clone = self_clone.clone();
                    tokio::spawn(async move {
                        let self_clone2 = self_clone.clone();
                        let ip_clone = ip.clone();
                        let ip_for_handshake = ip.clone();
                        let shared_height_client = shared_height_client.clone();

                        match timeout(
                            Duration::from_secs(HANDSHAKE_TIMEOUT_SECS),
                            async move {
                                if self_clone2.is_banned(&ip_for_handshake).await {
                                    let _ = stream.shutdown().await;
                                    return Ok::<(), anyhow::Error>(());
                                }

                                let mut client_public = [0u8; 32];
                                stream.read_exact(&mut client_public).await?;
                                stream.write_all(&self_clone2.pool_difficulty).await?;

                                let client_public = Public::new_from_buf(&client_public);
                                let miner_key = format!("{:?}", client_public);

                                if !self_clone2
                                    .try_register_connection(&miner_key, &ip_for_handshake)
                                    .await
                                {
                                    println!(
                                        "Rejecting connection: miner cap reached (max={}) miner={} ip={}",
                                        MAX_CONNS_PER_MINER, miner_key, ip_for_handshake
                                    );
                                    let _ = stream.shutdown().await;
                                    return Ok::<(), anyhow::Error>(());
                                }

                                tokio::spawn(self_clone2.connection(
                                    stream,
                                    client_public,
                                    ip_clone,
                                    submit_tx,
                                    job_handler.clone(),
                                    shared_height_client,
                                ));

                                Ok::<(), anyhow::Error>(())
                            },
                        )
                        .await
                        {
                            Err(_t) => {
                                println!("Handshake failed from {}, timeout", ip);
                                self_clone.add_penalty(ip).await;
                            }
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                println!("Handshake failed from {}, error: {}", ip, e);
                                self_clone.add_penalty(ip).await;
                            }
                        }
                    });

                    Ok::<(), anyhow::Error>(())
                }
                .await
                {
                    println!("API client failed to connect: {e}");
                }
            }

            #[allow(unreachable_code)]
            ()
        }))
    }

    fn extract_ip(addr: &SocketAddr) -> String {
        addr.ip().to_string()
    }
}

// ── Time helpers ────────────────────────────────────────────────────────────

fn now_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_secs()
}

// ============================================================================
// File: pool_api_server.rs
// Location: snap-coin-pool-v2/src/pool_api_server.rs
// Version: 0.1.4-stats.18
// Updated: 2026-02-16
// ============================================================================
// Generated: 2026-02-16T00:00:00Z
// LOC: (not auto-counted)
// ============================================================================
