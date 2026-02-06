use anyhow::anyhow;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

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

// ---------------- RATE LIMITER ----------------

#[derive(Clone)]
pub struct RateLimiter {
    limits: Arc<tokio::sync::Mutex<HashMap<Public, (u32, Instant)>>>,
    max_per_sec: u32,
}

impl RateLimiter {
    pub fn new(max_per_sec: u32) -> Self {
        Self {
            limits: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            max_per_sec,
        }
    }

    pub async fn allow(&self, key: &Public) -> bool {
        let mut map = self.limits.lock().await;
        let now = Instant::now();

        let entry = map.entry(key.clone()).or_insert((0, now));

        if now.duration_since(entry.1) > Duration::from_secs(1) {
            entry.0 = 0;
            entry.1 = now;
        }

        entry.0 += 1;
        entry.0 <= self.max_per_sec
    }
}

// ---------------- SERVER ----------------

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

    // ✅ added
    rate_limiter: RateLimiter,
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

            // ✅ added
            rate_limiter: RateLimiter::new(25),
        }
    }

    /// Handle a incoming connection
    async fn connection(self: Arc<Self>, mut stream: TcpStream, client_address: Public) {
        println!("New miner connected!");
        loop {
            if let Err(e) = async {
                // ✅ ONLY CHANGE: rate limit check
                if !self.rate_limiter.allow(&client_address).await {
                    return Err(anyhow!("Rate limit exceeded"));
                }

                let request = Request::decode_from_stream(&mut stream).await?;
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
                        println!("rx: mempool");
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
                            println!("rx: block");
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
                    Request::Difficulty => {
                        println!("rx: diff");
                        Response::Difficulty {
                        transaction_difficulty: self.blockchain.get_transaction_difficulty(),
                        block_difficulty: self.blockchain.get_block_difficulty(),
                    }},
                    Request::BlockHeight { hash } => Response::BlockHeight {
                        height: self.blockchain.block_store().get_block_height_by_hash(hash),
                    },
                    Request::LiveTransactionDifficulty => {
                        println!("rx: live diff");
                        Response::LiveTransactionDifficulty {
                        live_difficulty: calculate_live_transaction_difficulty(
                            &self.blockchain.get_transaction_difficulty(),
                            self.node_state.mempool.mempool_size().await,
                        ),
                    }
                    },
                    Request::SubscribeToChainEvents => {
                        println!("rx: subscribe");
                        let mut rx = self.node_state.chain_events.subscribe();
                        loop {
                            match rx.recv().await {
                                Ok(event) => {
                                    let response = Response::ChainEvent { event };
                                    stream.write_all(&response.encode()?).await?;
                                }
                                Err(_) => break,
                            }
                        }

                        return Ok(());
                    }
                };
                let response_buf = response.encode()?;

                stream.write_all(&response_buf).await?;

                Ok::<(), anyhow::Error>(())
            }
            .await
            {
                println!("API client error: {}", e);
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

        let server = self.clone();

        Ok(tokio::spawn(async move {
            loop {
                if let Err(e) = async {
                    let (mut stream, _) = listener.accept().await?;

                    let mut client_public = [0u8; 32];
                    stream.read_exact(&mut client_public).await?;
                    stream.write_all(&server.pool_difficulty).await?;
                    stream
                        .write_all(server.pool_private.to_public().dump_buf())
                        .await?;

                    let client_public = Public::new_from_buf(&client_public);

                    let server_clone = server.clone();
                    tokio::spawn(async move {
                        server_clone.connection(stream, client_public).await;
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
}
