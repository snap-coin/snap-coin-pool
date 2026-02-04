use std::{net::SocketAddr, sync::Arc};

use dotenvy::dotenv;
use serde::Deserialize;
use snap_coin::{
    crypto::keys::{Private, Public},
    full_node::{connect_peer, create_full_node, ibd::ibd_blockchain},
};

use crate::{pool_api_server::PoolServer, share_store::ShareStore};

mod handle_block;
mod handle_share;
mod pool_api_server;
mod share_store;

fn de_array<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let v: Vec<u8> = serde_json::from_str(&s).map_err(serde::de::Error::custom)?;
    v.try_into()
        .map_err(|_| serde::de::Error::custom("Expected 32 bytes"))
}

#[derive(Deserialize)]
struct Config {
    pool_node: String,
    pool_private: String,
    pool_dev: String,
    #[serde(deserialize_with = "de_array")]
    pool_difficulty: [u8; 32],
    pool_fee: f64,
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv()?;

    let config: Config = envy::from_env()?;
    let pool_node: SocketAddr = config.pool_node.parse()?;
    let pool_private = Private::new_from_base36(&config.pool_private)
        .ok_or(anyhow::anyhow!("Could not parse pool private"))?;
    let pool_dev = Public::new_from_base36(&config.pool_dev)
        .ok_or(anyhow::anyhow!("Could not parse pool dev"))?;

    // Create and connect to the pool node
    let (blockchain, node_state) = create_full_node("./pool-node", false);
    let pool_peer = connect_peer(pool_node, &blockchain, &node_state).await?;

    // Start IBD
    *node_state.is_syncing.write().await = true;
    ibd_blockchain(pool_peer, blockchain.clone(), false).await?;
    *node_state.is_syncing.write().await = false;

    let share_store = ShareStore::new();

    let pool_api_server = Arc::new(pool_api_server::PoolServer::new(
        config.port,
        blockchain,
        node_state,
        config.pool_difficulty,
        pool_private,
        pool_dev,
        config.pool_fee,
        share_store,
    ));
    PoolServer::listen(pool_api_server).await?.await?;
    Ok(())
}
