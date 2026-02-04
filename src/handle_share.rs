use snap_coin::{
    core::{block::Block, blockchain::BlockchainError}, crypto::keys::Public, economics::{DEV_WALLET, calculate_dev_fee, get_block_reward}, full_node::{SharedBlockchain, node_state::SharedNodeState}
};
use num_bigint::BigUint;

use crate::share_store::SharedShareStore;

pub async fn handle_share(
    blockchain: &SharedBlockchain,
    node_state: &SharedNodeState,
    block: Block,
    client_address: Public,
    pool_difficulty: &[u8; 32],
    pool_public: Public,
    share_store: &SharedShareStore,
) -> Result<(), BlockchainError> {
    block.check_completeness()?;
    if block.meta.block_pow_difficulty != blockchain.get_block_difficulty() || block.meta.tx_pow_difficulty != blockchain.get_transaction_difficulty() {
        return Err(BlockchainError::Block(snap_coin::core::block::BlockError::DifficultyMismatch))
    }
    if BigUint::from_bytes_be(&block.meta.hash.unwrap().dump_buf()) > BigUint::from_bytes_be(pool_difficulty) {
        return Err(BlockchainError::Block(snap_coin::core::block::BlockError::BlockPowDifficultyIncorrect))
    }
    block.check_meta()?;

    let mempool = node_state.mempool.get_mempool().await;
    let mut found_reward_tx = false;
    for tx in block.transactions.iter() {
        if tx.inputs.is_empty() {
            if found_reward_tx {
                return Err(BlockchainError::RewardOverspend);
            }
            found_reward_tx = true;

            if tx.outputs.len() < 2 {
                return Err(BlockchainError::InvalidRewardTransaction);
            }

            if tx.transaction_id.is_none() {
                return Err(BlockchainError::RewardTransactionIdMissing);
            }

            if tx.outputs.iter().fold(0, |acc, output| acc + output.amount)
                != get_block_reward(blockchain.block_store().get_height())
            {
                return Err(BlockchainError::InvalidRewardTransactionAmount);
            }

            let mut has_dev_fee = false;
            let mut has_pool_output = false;
            for output in &tx.outputs {
                if output.receiver == DEV_WALLET
                    && output.amount
                        == calculate_dev_fee(get_block_reward(
                            blockchain.block_store().get_height(),
                        ))
                {
                    has_dev_fee = true;
                    break;
                } else {
                    if output.receiver != pool_public || has_pool_output {
                        return Err(BlockchainError::InvalidRewardTransaction);
                    }
                    has_pool_output = true;
                }
            }

            if !has_dev_fee {
                return Err(BlockchainError::NoDevFee);
            }
        } else if !mempool.contains(&tx) {
            return Err(BlockchainError::IncompleteBlock);
        }
    }
    if !found_reward_tx {
        return Err(BlockchainError::NoDevFee);
    }
    if (block.transactions.len() + 10) as f64 * 1.25f64 < (mempool.len() + 5) as f64 {
        // With some buffer, check if we have enough transactions in the block
        return Err(BlockchainError::IncompleteBlock);
    }

    share_store.award_share(client_address).await;

    Ok(())
}
