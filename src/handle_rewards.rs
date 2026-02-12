// ============================================================================
// File: handle_rewards.rs
// Location: snap-coin-pool-v2/src/handle_rewards.rs
// Version: 0.1.3-rewardsmetrics.4
//
// Description:
// Reward split + payout transaction creation for a mined block.
//
// Key behavior (preserved):
// - Finds the coinbase output that pays the pool address (pool_private.to_public()).
// - Splits reward among miners proportional to shares in share_store.
// - Reserves one output slot for pool_dev remainder.
// - Computes tx PoW and submits payout transaction to the node.
// - Clears shares only for miners paid in this tx.
// - Prints basic payout diagnostics for backend visibility.
//
// Added (observational only):
// - PayoutMetrics includes:
//     * payouts: per-miner payouts actually included in tx
//     * txid: payout transaction id (best-effort from tx.transaction_id)
//   This allows WS telemetry emission of payout txid + per-miner amounts.
//
// FIX (v0.1.3-rewardsmetrics.4):
// - Silence dead_code warnings for:
//     * PayoutMetrics fields (some are not currently read by callers)
//     * handle_rewards() (legacy entry point kept for compatibility)
//
// Notes:
// - Guard rails avoid divide-by-zero and “missing coinbase output” traps.
// - Uses integer math (u128) for payout proportions (avoids float edge cases).
// ============================================================================

use crate::share_store::SharedShareStore;
use snap_coin::{
    api::client::Client,
    blockchain_data_provider::BlockchainDataProviderError,
    core::{
        block::{Block, MAX_TRANSACTIONS_PER_BLOCK},
        blockchain::BlockchainError,
        transaction::{Transaction, TransactionId, TransactionInput, TransactionOutput},
    },
    crypto::keys::{Private, Public},
};
use tokio::task::spawn_blocking;

/// Structured payout summary for dashboard telemetry (observational).
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct PayoutMetrics {
    /// Coinbase amount credited to the pool address in the mined block.
    pub coinbase_amount: u64,
    /// Net amount intended for miners after fee (before any max-output truncation / rounding).
    pub amount_to_share: u64,
    /// Total shares in the share_store snapshot used for this payout.
    pub total_shares: u64,
    /// Number of miners paid in this payout tx.
    pub miners_paid: usize,
    /// Number of miners deferred due to MAX_TRANSACTIONS_PER_BLOCK limit.
    pub miners_deferred: usize,
    /// Sum of miner payouts actually included in this tx.
    pub awarded_total: u64,
    /// Remainder sent to pool_dev (includes fee + dust + any deferred share portion).
    pub dev_remainder: u64,
    /// Fee amount (coinbase_amount - amount_to_share). This is the configured pool fee.
    pub pool_fee_amount: u64,

    /// Payout transaction id (best-effort).
    /// Derived from `tx.transaction_id` after PoW compute (if present).
    pub txid: String,

    /// Per-miner payouts actually included in this payout tx (observational).
    /// (Public key, amount in atomic units)
    pub payouts: Vec<(Public, u64)>,
}

/// Backwards-compatible entry point (signature preserved).
#[allow(dead_code)]
pub async fn handle_rewards(
    client: &Client,
    new_block: Block,
    pool_private: Private,
    pool_dev: Public,
    share_store: &SharedShareStore,
    pool_fee: f64,
) -> Result<(), BlockchainDataProviderError> {
    let _ = handle_rewards_with_metrics(
        client,
        new_block,
        pool_private,
        pool_dev,
        share_store,
        pool_fee,
    )
    .await?;
    Ok(())
}

/// Metrics-returning variant used by the pool API server to emit PayoutComplete.
/// Returns:
/// - Ok(Some(metrics)) when a payout tx was created+submitted and shares cleared.
/// - Ok(None) when payout is skipped due to missing coinbase/pool output/0 shares/etc.
pub async fn handle_rewards_with_metrics(
    client: &Client,
    new_block: Block,
    pool_private: Private,
    pool_dev: Public,
    share_store: &SharedShareStore,
    pool_fee: f64,
) -> Result<Option<PayoutMetrics>, BlockchainDataProviderError> {
    // ── 1) Locate coinbase + pool reward output ────────────────────────────

    let pool_pub = pool_private.to_public();

    let mut reward_output_index: usize = 0;
    let mut reward_tx_id = TransactionId::new_from_buf([0u8; 32]);
    let mut amount_from_block: u64 = 0;
    let mut found_coinbase = false;
    let mut found_pool_output = false;

    for tx in new_block.transactions.iter() {
        if tx.inputs.is_empty() {
            found_coinbase = true;

            if let Some(tid) = tx.transaction_id {
                reward_tx_id = tid;
            } else {
                println!("[PAYOUT] coinbase tx missing transaction_id; skipping payout");
                return Ok(None);
            }

            for (i, output) in tx.outputs.iter().enumerate() {
                if output.receiver == pool_pub {
                    reward_output_index = i;
                    amount_from_block = output.amount;
                    found_pool_output = true;
                    break;
                }
            }

            break;
        }
    }

    if !found_coinbase {
        println!("[PAYOUT] no coinbase tx found in block; skipping payout");
        return Ok(None);
    }

    if !found_pool_output {
        println!(
            "[PAYOUT] coinbase found but no output to pool address {:?}; skipping payout",
            pool_pub
        );
        return Ok(None);
    }

    if amount_from_block == 0 {
        println!("[PAYOUT] coinbase amount to pool is 0; skipping payout");
        return Ok(None);
    }

    // ── 2) Pull shares + validate totals ───────────────────────────────────

    let received_shares = share_store.get_shares().await;
    let total_shares: u64 = received_shares.values().copied().sum();

    if total_shares == 0 {
        println!(
            "[PAYOUT] total_shares=0 (no eligible shares). amount_from_block={} — skipping payout",
            amount_from_block
        );
        return Ok(None);
    }

    // ── 3) Compute amount_to_share (pool fee) using safe integer math ──────

    let fee = if pool_fee.is_finite() { pool_fee } else { 0.0 };
    let fee = fee.clamp(0.0, 1.0);

    // Compute: amount_to_share = amount_from_block * (1 - fee)
    // Use fixed-point ppm to avoid float instability.
    let one_minus_fee_ppm: u128 = ((1.0 - fee) * 1_000_000.0) as u128;
    let amount_to_share_u128: u128 =
        (amount_from_block as u128) * one_minus_fee_ppm / 1_000_000u128;

    let amount_to_share: u64 = amount_to_share_u128.min(u64::MAX as u128) as u64;

    if amount_to_share == 0 {
        println!(
            "[PAYOUT] amount_to_share=0 after fee. amount_from_block={} fee={} — skipping payout",
            amount_from_block, fee
        );
        return Ok(None);
    }

    let pool_fee_amount = amount_from_block.saturating_sub(amount_to_share);

    // ── 4) Build payout list (proportional) ────────────────────────────────
    //
    // payout = shares / total_shares * amount_to_share
    // Uses u128 to avoid overflow and float artifacts.

    let mut output_shares: Vec<(Public, u64, u64)> = received_shares
        .iter()
        .map(|(contributor, shares)| {
            let payout_u128 =
                (*shares as u128) * (amount_to_share as u128) / (total_shares as u128);
            let payout = payout_u128.min(u64::MAX as u128) as u64;
            (*contributor, payout, *shares)
        })
        .collect();

    // Sort by shares descending (biggest contributors first).
    output_shares.sort_by(|a, b| b.2.cmp(&a.2));

    // Determine how many we can pay in this transaction (reserve 1 slot for pool_dev remainder).
    let max_payouts = (MAX_TRANSACTIONS_PER_BLOCK - 1).min(output_shares.len());

    let (to_pay_now, to_defer) = output_shares.split_at(max_payouts);

    let awarded_total: u64 = to_pay_now.iter().map(|(_, amount, _)| *amount).sum();

    // Remainder to dev (fee + dust + any deferred portion).
    let dev_remainder = amount_from_block.saturating_sub(awarded_total);

    // Build final outputs.
    let mut final_outputs: Vec<(Public, u64)> =
        to_pay_now.iter().map(|(p, a, _)| (*p, *a)).collect();
    final_outputs.push((pool_dev, dev_remainder));

    println!(
        "[PAYOUT] coinbase_amount={} amount_to_share={} total_shares={} pay_now={} defer={} dev_remainder={}",
        amount_from_block,
        amount_to_share,
        total_shares,
        to_pay_now.len(),
        to_defer.len(),
        dev_remainder
    );

    // ── 5) Create + sign payout transaction ────────────────────────────────

    let mut tx = Transaction::new_transaction_now(
        vec![TransactionInput {
            transaction_id: reward_tx_id,
            output_index: reward_output_index,
            output_owner: pool_pub,
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

    // ── 6) Compute tx PoW + submit ─────────────────────────────────────────

    let tx_diff = client.get_live_transaction_difficulty().await?;
    let tx = spawn_blocking(move || {
        tx.compute_pow(&tx_diff, Some(0.1))
            .map_err(|e| BlockchainError::BincodeEncode(e.to_string()))?;
        Ok::<_, BlockchainError>(tx)
    })
    .await
    .map_err(|_| BlockchainError::Io("Could not compute pow for reward split tx".to_string()))??;

    // Capture best-effort txid (observational)
    let txid: String = tx
        .transaction_id
        .map(|id| format!("{:?}", id))
        .unwrap_or_else(|| "-".to_string());

    client.submit_transaction(tx).await??;

    println!(
        "[PAYOUT] submit_transaction OK (paid_miners={})",
        to_pay_now.len()
    );

    // ── 7) Clear shares only for those who were paid ───────────────────────

    let paid_miners: Vec<Public> = to_pay_now.iter().map(|(public, _, _)| *public).collect();
    share_store.clear_shares_for(&paid_miners).await;

    if !to_defer.is_empty() {
        println!(
            "[PAYOUT] Deferred payouts for {} miners (will be paid in next block)",
            to_defer.len()
        );
    }

    // Observational per-miner payouts included in this tx
    let payouts: Vec<(Public, u64)> = to_pay_now
        .iter()
        .map(|(public, amount, _shares)| (*public, *amount))
        .collect();

    Ok(Some(PayoutMetrics {
        coinbase_amount: amount_from_block,
        amount_to_share,
        total_shares,
        miners_paid: to_pay_now.len(),
        miners_deferred: to_defer.len(),
        awarded_total,
        dev_remainder,
        pool_fee_amount,
        txid,
        payouts,
    }))
}

// ============================================================================
// File: handle_rewards.rs
// Location: snap-coin-pool-v2/src/handle_rewards.rs
// Version: 0.1.3-rewardsmetrics.4
// Updated: 2026-02-12
// ============================================================================
