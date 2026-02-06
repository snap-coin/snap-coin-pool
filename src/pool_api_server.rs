use anyhow::anyhow;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use tokio::sync::Mutex;

use snap_coin::{
    api::requests::{Request, Response},
    core::{difficulty::calculate_live_transaction_difficulty, utils::slice_vec},
    crypto::keys::{Private, Public},
    economics::get_block_reward,
    full_node::{SharedBlockchain, accept_block, node_state::SharedNodeState},
};

use crate::{
    handle_block::handle_block, handle_share::handle_share, share_store::SharedShareStore,
};

pub const PAGE_SIZE: u32 = 200;
const ERROR_PENALTY: i32 = 5;
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
    blockchain: SharedBlockchain,
    node_state: SharedNodeState,
    pool_difficulty: [u8; 32],
    pool_private: Private,
    pool_dev: Public,
    pool_fee: f64,
    share_store: SharedShareStore,
    ip_scores: IpScoreMap,
}

impl PoolServer {
    /// Create a new server, do not listen for connections yet
    pub fn new(
        port: u16,
        blockchain: SharedBlockchain,
        node_state: SharedNodeState,
        pool_difficulty: [u8; 32],
        pool_private: Private,
        pool_dev: Public,
        pool_fee: f64,
        share_store: SharedShareStore,
    ) -> Self {
        PoolServer {
            port,
            blockchain,
            node_state,
            pool_difficulty,
            pool_private,
            pool_dev,
            pool_fee,
            share_store,
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
        self: Arc<Self>,
        mut stream: TcpStream,
        client_address: Public,
        ip: String,
    ) {
        loop {
            if let Err(e) = async {
                let request = Request::decode_from_stream(&mut stream).await?;
                println!("Got request {}", ip);
                let response = match request {
                    Request::Height => Response::Height {
                        height: self.blockchain.block_store().get_height() as u64,
                    },
                    Request::Block { block_hash } => Response::Block {
                        block: self.blockchain.block_store().get_block_by_hash(block_hash),
                    },
                    Request::BlockHash { height } => Response::BlockHash {
                        hash: self
                            .blockchain
                            .block_store()
                            .get_block_hash_by_height(height as usize),
                    },
                    Request::Transaction { .. } => {
                        return Err(anyhow!("Restricted API endpoint accessed"));
                    }
                    Request::TransactionAndInfo { .. } => {
                        return Err(anyhow!("Restricted API endpoint accessed"));
                    }
                    Request::TransactionsOfAddress { .. } => {
                        return Err(anyhow!("Restricted API endpoint accessed"));
                    }
                    Request::AvailableUTXOs { .. } => {
                        return Err(anyhow!("Restricted API endpoint accessed"));
                    }
                    Request::Balance { .. } => {
                        return Err(anyhow!("Restricted API endpoint accessed"));
                    }
                    Request::Reward => Response::Reward {
                        reward: get_block_reward(self.blockchain.block_store().get_height()),
                    },
                    Request::Peers => return Err(anyhow!("Restricted API endpoint accessed")),
                    Request::Mempool {
                        page: requested_page,
                    } => {
                        let mempool = self.node_state.mempool.get_mempool().await;
                        let page = slice_vec(
                            &mempool,
                            (requested_page * PAGE_SIZE) as usize,
                            ((requested_page + 1) * PAGE_SIZE) as usize,
                        );
                        let next_page = if page.len() != PAGE_SIZE as usize {
                            None
                        } else {
                            Some(requested_page + 1)
                        };

                        Response::Mempool {
                            mempool: page.to_vec(),
                            next_page,
                        }
                    }
                    Request::NewBlock { new_block } => Response::NewBlock {
                        status: {
                            let res = handle_share(
                                &self.blockchain,
                                &self.node_state,
                                new_block.clone(),
                                client_address,
                                &self.pool_difficulty,
                                self.pool_private.to_public(),
                                &self.share_store,
                            )
                            .await;
                            if res.is_ok() {
                                if new_block
                                    .validate_difficulties(
                                        &self.blockchain.get_block_difficulty(),
                                        &self.blockchain.get_transaction_difficulty(),
                                    )
                                    .is_ok()
                                {
                                    let res = accept_block(
                                        &self.blockchain,
                                        &self.node_state,
                                        new_block.clone(),
                                    )
                                    .await;
                                    if res.is_ok() {
                                        println!(
                                            "New block mined by pool: {:?}",
                                            handle_block(
                                                &self.node_state,
                                                &self.blockchain,
                                                new_block,
                                                self.pool_private,
                                                self.pool_dev,
                                                &self.share_store,
                                                self.pool_fee
                                            )
                                            .await
                                        );
                                    }
                                }
                            }

                            res
                        },
                    },
                    Request::NewTransaction { .. } => {
                        return Err(anyhow!("Restricted API endpoint accessed"));
                    }
                    Request::Difficulty => Response::Difficulty {
                        transaction_difficulty: self.blockchain.get_transaction_difficulty(),
                        block_difficulty: self.blockchain.get_block_difficulty(),
                    },
                    Request::BlockHeight { hash } => Response::BlockHeight {
                        height: self.blockchain.block_store().get_block_height_by_hash(hash),
                    },
                    Request::LiveTransactionDifficulty => Response::LiveTransactionDifficulty {
                        live_difficulty: calculate_live_transaction_difficulty(
                            &self.blockchain.get_transaction_difficulty(),
                            self.node_state.mempool.mempool_size().await,
                        ),
                    },
                    Request::SubscribeToChainEvents => {
                        let mut rx = self.node_state.chain_events.subscribe();
                        // Start event stream task
                        loop {
                            match rx.recv().await {
                                Ok(event) => {
                                    let response = Response::ChainEvent { event };
                                    stream.write_all(&response.encode()?).await?;
                                }
                                Err(_) => break,
                            }
                        }

                        // Stop request response task
                        return Err(anyhow!("Request response loop ended"));
                    }
                };
                let response_buf = response.encode()?;

                println!("Sending response {}", ip);
                stream.write_all(&response_buf).await?;

                Ok::<(), anyhow::Error>(())
            }
            .await
            {
                println!("API client error from {}: {}", ip, e);
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

        let server = self.clone();

        Ok(tokio::spawn(async move {
            loop {
                let server = server.clone();
                if let Err(e) = async {
                    let (mut stream, addr) = listener.accept().await?;
                    let ip = Self::extract_ip(&addr);

                    tokio::spawn(async move {
                        // Check if IP is banned
                        if server.is_banned(&ip).await {
                            let _ = stream.shutdown().await;
                            return;
                        }

                        let server_clone = server.clone();
                        let ip_clone = ip.clone();
                        match timeout(Duration::from_secs(1), async move {
                            let mut client_public = [0u8; 32];
                            stream.read_exact(&mut client_public).await?;
                            stream.write_all(&server_clone.pool_difficulty).await?;
                            stream
                                .write_all(server_clone.pool_private.to_public().dump_buf())
                                .await?;

                            let client_public = Public::new_from_buf(&client_public);
                            server_clone
                                .connection(stream, client_public, ip_clone.clone())
                                .await;
                            Ok::<(), anyhow::Error>(())
                        })
                        .await
                        {
                            Err(_t) => {
                                println!("Handshake failed from {}, timeout", ip);
                                server.add_penalty(ip).await;
                            }
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => {
                                println!("Handshake failed from {}, error: {}", ip, e);
                                server.add_penalty(ip).await;
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
