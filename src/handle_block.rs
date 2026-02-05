use snap_coin::{
    core::{
        block::{Block, MAX_TRANSACTIONS_PER_BLOCK},
        blockchain::BlockchainError,
        transaction::{Transaction, TransactionId, TransactionInput, TransactionOutput},
    },
    crypto::keys::{Private, Public},
    full_node::{SharedBlockchain, accept_transaction, node_state::SharedNodeState},
};
use tokio::task::spawn_blocking;
use crate::share_store::SharedShareStore;

pub async fn handle_block(
    node_state: &SharedNodeState,
    blockchain: &SharedBlockchain,
    block: Block,
    pool_private: Private,
    pool_dev: Public,
    share_store: &SharedShareStore,
    pool_fee: f64,
) -> Result<(), BlockchainError> {
    let mut reward_output_index = 0;
    let mut reward_tx_id = TransactionId::new_from_buf([0u8; 32]);
    let mut amount_from_block = 0;
    
    for tx in block.transactions.iter() {
        if tx.inputs.is_empty() {
            reward_tx_id = tx.transaction_id.unwrap();
            for (i, output) in tx.outputs.iter().enumerate() {
                if output.receiver == pool_private.to_public() {
                    reward_output_index = i;
                    amount_from_block = output.amount;
                }
            }
        }
    }
    
    let amount_to_share = (amount_from_block as f64 * (1f64 - pool_fee)) as u64;
    let received_shares = share_store.get_shares().await;
    
    let total_shares: u64 = received_shares.values().sum();
    
    // Calculate payouts for all contributors
    let mut output_shares: Vec<(Public, u64)> = received_shares
        .iter()
        .map(|(contributor, shares)| {
            let payout = (((*shares as f64) / (total_shares as f64)) * (amount_to_share as f64)) as u64;
            (*contributor, payout)
        })
        .collect();
    
    // Sort by shares (descending) - highest contributors first
    output_shares.sort_by(|(_, a), (_, b)| b.cmp(a));
    
    // Determine how many we can pay in this transaction
    // Reserve 1 slot for pool dev fee
    let max_payouts = (MAX_TRANSACTIONS_PER_BLOCK - 1).min(output_shares.len());
    
    // Split into those we'll pay now and those we'll defer
    let (to_pay_now, to_defer) = output_shares.split_at(max_payouts);
    
    let awarded_total: u64 = to_pay_now.iter().map(|(_, amount)| amount).sum();
    
    // Build final output list with pool dev fee
    let mut final_outputs: Vec<(Public, u64)> = to_pay_now.to_vec();
    final_outputs.push((pool_dev, amount_from_block - awarded_total));
    
    // Create and sign the transaction
    let mut tx = Transaction::new_transaction_now(
        vec![TransactionInput {
            transaction_id: reward_tx_id,
            output_index: reward_output_index,
            output_owner: pool_private.to_public(),
            signature: None,
        }],
        final_outputs
            .iter()
            .map(|(r, a)| TransactionOutput {
                amount: *a,
                receiver: *r,
            })
            .collect(),
        &mut vec![pool_private],
    )
    .map_err(|e| BlockchainError::BincodeEncode(e.to_string()))?;
    
    let tx_diff = blockchain.get_transaction_difficulty();
    let tx = spawn_blocking(move || {
        tx.compute_pow(&tx_diff, Some(0.1))
            .map_err(|e| BlockchainError::BincodeEncode(e.to_string()))?;
        Ok::<_, BlockchainError>(tx)
    })
    .await
    .map_err(|_| BlockchainError::Io("Could not compute pow for reward split tx".to_string()))??;
    
    accept_transaction(&blockchain, &node_state, tx).await?;
    
    // Clear shares only for those who were paid
    let paid_miners: Vec<Public> = to_pay_now.iter().map(|(public, _)| *public).collect();
    share_store.clear_shares_for(&paid_miners).await;
    
    // Log if we deferred any payouts
    if !to_defer.is_empty() {
        println!(
            "⚠️  Deferred payouts for {} miners (will be paid in next block)",
            to_defer.len()
        );
    }
    
    Ok(())
}