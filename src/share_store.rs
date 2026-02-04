use std::{collections::HashMap, sync::Arc};

use snap_coin::crypto::keys::Public;
use tokio::sync::RwLock;

pub type SharedShareStore = Arc<ShareStore>;

pub struct ShareStore {
    shares: RwLock<HashMap<Public, u64>>,
}

impl ShareStore {
    pub fn new() -> SharedShareStore {
        Arc::new(Self {
            shares: RwLock::new(HashMap::new()),
        })
    }

    pub async fn award_share(&self, miner: Public) {
        *self.shares.write().await.entry(miner).or_insert(0) += 1;
    }

    pub async fn clear_shares(&self) {
        self.shares.write().await.clear();
    }

    pub async fn get_shares(&self) -> HashMap<Public, u64> {
        self.shares.read().await.clone()
    }
}
