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

use crate::{
    handle_rewards::handle_rewards, handle_share::handle_share, job_handler::JobHandler,
    share_store::SharedShareStore,
};

const ERROR_PENALTY: i32 = 1;
const BAN_THRESHOLD: i32 = 25;
const BAN_PENALTY: i32 = 10;
const SCORE_DECAY_INTERVAL: Duration = Duration::from_secs(30);
const SCORE_DECAY_AMOUNT: i32 = 1;

#[derive(Clone)]
struct IpScore {
    score: i32,
    banned: bool,
}

type IpScoreMap = Arc<Mutex<HashMap<String, IpScore>>>;

/// Server for hosting a Snap Coin API
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
}

impl PoolServer {
    /// Create a new server, do not listen for connections yet
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
        }
    }

    /// Add penalty to an IP address
    async fn add_penalty(&self, ip: String) {
        let mut scores = self.ip_scores.lock().await;
        let entry = scores.entry(ip.clone()).or_insert(IpScore {
            score: 0,
            banned: false,
        });

        entry.score += ERROR_PENALTY;

        if entry.score >= BAN_THRESHOLD && !entry.banned {
            entry.score += BAN_PENALTY;
            entry.banned = true;
            println!("IP {} has been BANNED (score: {})", ip, entry.score);
        }
    }

    /// Check if an IP is banned
    async fn is_banned(&self, ip: &str) -> bool {
        let scores = self.ip_scores.lock().await;
        scores.get(ip).map(|entry| entry.banned).unwrap_or(false)
    }

    /// Periodically decay scores for all IPs
    async fn score_decay_task(ip_scores: IpScoreMap) {
        let mut interval = tokio::time::interval(SCORE_DECAY_INTERVAL);
        loop {
            interval.tick().await;
            let mut scores = ip_scores.lock().await;

            scores.retain(|ip, entry| {
                entry.score = (entry.score - SCORE_DECAY_AMOUNT).max(0);

                // Unban if score drops below threshold
                if entry.banned && entry.score < BAN_THRESHOLD {
                    entry.banned = false;
                    println!("IP {} has been unbanned (score: {})", ip, entry.score);
                }

                // Remove entry if score is 0 and not banned
                entry.score > 0 || entry.banned
            });
        }
    }

    /// Handle a incoming connection
    async fn connection(
        self: Arc<PoolServer>,
        mut stream: TcpStream,
        client_address: Public,
        ip: String,
        submit_block: mpsc::Sender<(Block, Public)>,
        job_handler: Arc<JobHandler>,
    ) {
        loop {
            if let Err(e) = async {
                let request = Request::decode_from_stream(&mut stream).await?;
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

                        // Start event stream task
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

                        // Stop request response task
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
                println!("Miner error from {}: {}", ip, e);
                self.add_penalty(ip.clone()).await;
                break;
            }
        }
    }

    /// Start listening for clients
    pub async fn listen(self: Arc<Self>) -> Result<JoinHandle<()>, anyhow::Error> {
        let listener = match TcpListener::bind(format!("0.0.0.0:{}", self.port)).await {
            Ok(l) => l,
            Err(_) => TcpListener::bind("0.0.0.0:0").await?,
        };
        println!(
            "Pool API Server listening on 0.0.0.0:{}",
            listener.local_addr()?.port()
        );

        // Start score decay task
        let ip_scores_clone = self.ip_scores.clone();
        tokio::spawn(async move {
            Self::score_decay_task(ip_scores_clone).await;
        });

        let (submit_tx, mut submit_rx) = mpsc::channel(24);
        let self_clone = self.clone();
        tokio::spawn(async move {
            let submit_client = Arc::new(Client::connect(self_clone.pool_api).await.unwrap());
            loop {
                let submit: Option<(Block, Public)> = submit_rx.recv().await;
                let self_clone = self_clone.clone();
                let submit_client = submit_client.clone();
                if let Some((block, _public)) = submit {
                    if let Err(e) = async move {
                        submit_client.submit_block(block.clone()).await??;
                        println!(
                            "[POOL] Mined new valid block! {}",
                            block.meta.hash.unwrap().dump_base36()
                        );
                        println!(
                            "[POOL] Pool reward transaction status: {:?}",
                            handle_rewards(
                                &*submit_client,
                                block,
                                self_clone.pool_private,
                                self_clone.pool_dev,
                                &self_clone.share_store,
                                self_clone.pool_fee,
                            )
                            .await
                        );

                        Ok::<(), anyhow::Error>(())
                    }
                    .await
                    {
                        println!("Failed to submit pool block: {e}")
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
                    println!("Job updater failed! {e}")
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
                if let Err(e) = async move {
                    let ip = Self::extract_ip(&addr);

                    let self_clone = self_clone.clone();
                    tokio::spawn(async move {
                        // Check if IP is banned
                        if self_clone.is_banned(&ip).await {
                            let _ = stream.shutdown().await;
                            return;
                        }

                        let self_clone = self_clone.clone();
                        let self_clone2 = self_clone.clone();
                        let ip_clone = ip.clone();
                        match timeout(Duration::from_secs(5), async move {
                            let mut client_public = [0u8; 32];
                            stream.read_exact(&mut client_public).await?;
                            stream.write_all(&self_clone.pool_difficulty).await?;

                            let client_public = Public::new_from_buf(&client_public);
                            tokio::spawn(self_clone.connection(
                                stream,
                                client_public,
                                ip_clone,
                                submit_tx,
                                job_handler.clone(),
                            ));
                            Ok::<(), anyhow::Error>(())
                        })
                        .await
                        {
                            Err(_t) => {
                                println!("Handshake failed from {}, timeout", ip);
                                self_clone2.add_penalty(ip).await;
                            }
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                println!("Handshake failed from {}, error: {}", ip, e);
                                self_clone2.add_penalty(ip).await;
                            }
                        }
                    });

                    Ok::<(), anyhow::Error>(())
                }
                .await
                {
                    println!("API client failed to connect: {e}")
                }
            }

            #[allow(unreachable_code)]
            ()
        }))
    }

    /// Extract IP address from SocketAddr
    fn extract_ip(addr: &SocketAddr) -> String {
        addr.ip().to_string()
    }
}
