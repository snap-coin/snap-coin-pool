use num_bigint::BigUint;
use snap_coin::{
    core::{
        block::{Block, BlockError},
        blockchain::BlockchainError,
        transaction::TransactionId,
    },
    crypto::{
        address_inclusion_filter::AddressInclusionFilter, keys::Public, merkle_tree::MerkleTree,
    },
    economics::EXPIRATION_TIME,
};

use crate::share_store::SharedShareStore;

pub async fn handle_share(
    current_job: &Block,
    new_block: &Block,
    share_store: &SharedShareStore,
    client_address: Public,
    pool_difficulty: &[u8; 32],
) -> Result<(), BlockchainError> {
    new_block.check_completeness()?;

    let mut current_job = current_job.clone();

    // Remove expired transactions with 10s margin
    let mut removed_txs = false;
    current_job.transactions.retain(|tx| {
        let expired = tx.timestamp + EXPIRATION_TIME + 10 < chrono::Utc::now().timestamp() as u64;
        if expired {
            removed_txs = true;
        }
        !expired
    });

    // Update merkle tree and filter if transactions were removed
    if removed_txs {
        current_job.meta.merkle_tree_root = MerkleTree::build(
            &current_job
                .transactions
                .iter()
                .map(|tx| tx.transaction_id.unwrap())
                .collect::<Vec<TransactionId>>(),
        )
        .root_hash();
        current_job.meta.address_inclusion_filter =
            AddressInclusionFilter::create_filter(&current_job.transactions)
                .map_err(|e| BlockchainError::from(BlockError::from(e)))?;
    }

    if current_job.transactions != new_block.transactions {
        return Err(BlockchainError::IncompleteBlock);
    }

    let mut job_meta = current_job.meta.clone();
    let mut new_block_meta = new_block.meta.clone();
    job_meta.hash = None;
    new_block_meta.hash = None;

    if job_meta != new_block_meta {
        return Err(BlockchainError::IncompleteBlock);
    }

    if BigUint::from_bytes_be(&new_block.meta.hash.unwrap().dump_buf())
        > BigUint::from_bytes_be(pool_difficulty)
    {
        return Err(BlockError::BlockPowDifficultyIncorrect.into());
    }

    share_store.award_share(client_address).await;
    Ok(())
}
