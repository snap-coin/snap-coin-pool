use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use snap_coin::{
    api::{api_server::ApiError, client::Client},
    blockchain_data_provider::BlockchainDataProviderError,
    build_block,
    core::{
        block::{Block, MAX_TRANSACTIONS_PER_BLOCK},
        transaction::Transaction,
        utils::slice_vec,
    },
    crypto::keys::Private,
    economics::EXPIRATION_TIME,
};
use tokio::sync::broadcast;

async fn get_current_mempool(
    client: &Client,
) -> Result<Vec<Transaction>, BlockchainDataProviderError> {
    let mut mempool = slice_vec(
        &client.get_mempool().await?,
        0,
        MAX_TRANSACTIONS_PER_BLOCK - 1,
    )
    .to_vec();
    mempool.retain(|tx| tx.timestamp + 5 < EXPIRATION_TIME + chrono::Utc::now().timestamp() as u64); // Add a 5s expiration buffer
    Ok(mempool)
}

pub struct JobHandler {
    tx_subscriber: broadcast::Sender<Block>,
}

impl JobHandler {
    pub async fn listen(
        node_api: SocketAddr,
        pool_private: Private,
    ) -> Result<(Self, Block), ApiError> {
        let event_client = Client::connect(node_api).await?;
        let job_client = Arc::new(Client::connect(node_api).await?);

        let (job_tx, _job_rx) = broadcast::channel::<Block>(24);

        let tx_subscriber = job_tx.clone();
        let job_tx = Arc::new(job_tx);
        let is_building = Arc::new(AtomicBool::new(false));

        let first_job = build_job(&job_client, &is_building, pool_private, &job_tx)
            .await
            .expect("Could not get first job!"); // Build first job before events

        tokio::spawn(async move {
            event_client
                .convert_to_event_listener(move |_event| {
                    let is_building = is_building.clone();
                    let job_client = job_client.clone();
                    let job_tx = job_tx.clone();
                    tokio::spawn(async move {
                        build_job(&job_client, &is_building, pool_private, &job_tx).await;
                        is_building.store(false, Ordering::Relaxed);
                    });
                })
                .await
                .unwrap();
        });

        Ok((JobHandler { tx_subscriber }, first_job))
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Block> {
        self.tx_subscriber.subscribe()
    }
}

async fn build_job(
    job_client: &Client,
    is_building: &Arc<AtomicBool>,
    pool_private: Private,
    job_tx: &broadcast::Sender<Block>,
) -> Option<Block> {
    if is_building.load(Ordering::Relaxed) {
        return None;
    }
    is_building.store(true, Ordering::Relaxed);
    Some(
        match async move {
            let block = build_block(
                &*job_client,
                &get_current_mempool(&*job_client).await?,
                pool_private.to_public(),
            )
            .await?;

            let _ = job_tx.send(block.clone());

            Ok::<Block, anyhow::Error>(block)
        }
        .await
        {
            Ok(block) => block,
            Err(e) => {
                println!("[JOB] Job Handler failed: {}", e);
                return None;
            }
        },
    )
}
