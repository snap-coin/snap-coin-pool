// ============================================================================
// File: main.rs
// Location: snap-coin-pool-v2/src/main.rs
// Version: 0.1.0-stats.1
//
// Description:
// Entrypoint for Snap Coin pool.
// - Loads config from env via dotenv + envy
// - Starts PoolStatsServer (dashboard + WS feed) on stats_port
// - Starts PoolServer (miner API) on port
//
// Notes:
// - stats_port is optional; defaults to (port + 1) if not provided.
// ============================================================================

use std::{net::SocketAddr, sync::Arc};

use dotenvy::dotenv;
use serde::Deserialize;
use snap_coin::crypto::keys::{Private, Public};

use crate::{
    pool_api_server::PoolServer,
    pool_stats_server::PoolStatsServer,
    share_store::ShareStore,
};

mod handle_rewards;
mod handle_share;
mod job_handler;
mod pool_api_server;
mod share_store;
mod pool_stats_server;
mod poolstate;


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
    pool_api: String,
    pool_private: String,
    pool_dev: String,
    #[serde(deserialize_with = "de_array")]
    pool_difficulty: [u8; 32],
    pool_fee: f64,
    port: u16,

    // OPTIONAL: separate port for dashboard + websocket stats feed
    // If omitted, defaults to port + 1
    stats_port: Option<u16>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv()?;

    let config: Config = envy::from_env()?;
    let pool_api: SocketAddr = config.pool_api.parse()?;

    let pool_private = Private::new_from_base36(&config.pool_private)
        .ok_or(anyhow::anyhow!("Could not parse pool private"))?;

    let pool_dev = Public::new_from_base36(&config.pool_dev)
        .ok_or(anyhow::anyhow!("Could not parse pool dev"))?;

    let stats_port = config.stats_port.unwrap_or(config.port.saturating_add(1));

    // Start stats server (dashboard + websocket feed)
    let (stats_server, stats_sender) = PoolStatsServer::new(stats_port).await;
    tokio::spawn(async move {
        if let Err(e) = stats_server.listen().await {
            eprintln!("[STATS] server failed: {e}");
        }
    });

    let share_store = ShareStore::new();

    // Construct pool server (unchanged signature), then attach stats sender
    let mut pool_server = PoolServer::new(
        config.port,
        config.pool_difficulty,
        pool_private,
        pool_dev,
        config.pool_fee,
        pool_api,
        share_store,
    );
    pool_server.set_stats_sender(stats_sender);

    let pool_api_server = Arc::new(pool_server);

    pool_api_server.listen().await?.await?;
    Ok(())
}

// ============================================================================
// File: main.rs
// Location: snap-coin-pool-v2/src/main.rs
// Version: 0.1.0-stats.1
// Updated: 2026-02-10
// ============================================================================
