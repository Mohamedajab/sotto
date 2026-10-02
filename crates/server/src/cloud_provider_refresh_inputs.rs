//! Durable inputs for one provider refresh job.
//!
//! A queue row stores stable ids so it remains small and provider-neutral. This loader joins the
//! receipt, allocation, payer, and source records after a lease is claimed, then reconstructs the
//! validated provider values used by the refresh lifecycle. It never returns raw payload bytes.

use sqlx::{PgPool, Row};
use thiserror::Error;

use crate::cloud_provider::{
    AllocationState, PayerKind, VerifiedAllocation, VerifiedProviderEvent,
};
use crate::cloud_provider_refresh_jobs::RefreshJobLease;

#[derive(Debug, Error)]
pub enum RefreshInputError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("refresh job inputs are missing")]
    Missing,
    #[error("refresh job inputs are corrupt: {0}")]
    Corrupt(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshJobInputs {
    pub lease: RefreshJobLease,
    pub event: VerifiedProviderEvent,
    pub allocation: VerifiedAllocation,
    pub receipt_status: String,
}

/// Load and validate all durable inputs for a claimed refresh job.
pub async fn load(
    pool: &PgPool,
    lease: &RefreshJobLease,
) -> Result<RefreshJobInputs, RefreshInputError> {
    let row = sqlx::query(
        "SELECT receipt.event_type, receipt.provider_created_at, receipt.subscription_id, \
                receipt.allocation_reference, receipt.normalized_payload_hash, receipt.status, \
                allocation.payer_id, allocation.provider_customer_id, payer.payer_kind, \
                allocation.provider_subscription_id, allocation.provider_item_id, \
                allocation.external_allocation_reference, allocation.effective_from, \
                allocation.effective_until, allocation.state, allocation.ownership_evidence_reference \
         FROM cloud_provider_event_receipts AS receipt \
         JOIN cloud_provider_allocations AS allocation \
           ON allocation.allocation_id = $5 \
         JOIN cloud_provider_payers AS payer ON payer.payer_id = allocation.payer_id \
         JOIN cloud_coverage_sources AS source \
           ON source.source_id = allocation.coverage_source_id \
         WHERE receipt.provider_namespace = $1 \
           AND receipt.provider_account_id = $2 \
           AND receipt.provider_environment = $3 \
           AND receipt.event_id = $4 \
           AND allocation.provider_namespace = $1 \
           AND allocation.provider_account_id = $2 \
           AND allocation.provider_environment = $3 \
           AND allocation.beneficiary_id = $6 \
           AND allocation.coverage_source_id = $7 \
           AND source.beneficiary_id = allocation.beneficiary_id \
           AND payer.provider_namespace = $1 \
           AND payer.provider_account_id = $2 \
           AND payer.provider_environment = $3",
    )
    .bind(&lease.context.namespace)
    .bind(&lease.context.account_id)
    .bind(lease.context.environment.as_str())
    .bind(&lease.event_id)
    .bind(&lease.allocation_id)
    .bind(&lease.beneficiary_id)
    .bind(&lease.source_id)
    .fetch_optional(pool)
    .await?
    .ok_or(RefreshInputError::Missing)?;

    let event = VerifiedProviderEvent::from_stored(
        lease.event_id.clone(),
        row.try_get("event_type")?,
        row.try_get("provider_created_at")?,
        row.try_get("subscription_id")?,
        row.try_get("allocation_reference")?,
        row.try_get("normalized_payload_hash")?,
    )
    .map_err(|error| RefreshInputError::Corrupt(error.to_string()))?;
    let payer_kind = parse_payer_kind(row.try_get::<String, _>("payer_kind")?.as_str())?;
    let state = parse_allocation_state(row.try_get::<String, _>("state")?.as_str())?;
    let allocation = VerifiedAllocation::new(
        lease.allocation_id.clone(),
        row.try_get::<String, _>("payer_id")?,
        row.try_get::<String, _>("provider_customer_id")?,
        payer_kind,
        lease.beneficiary_id.clone(),
        row.try_get::<String, _>("provider_subscription_id")?,
        row.try_get::<String, _>("provider_item_id")?,
        row.try_get::<String, _>("external_allocation_reference")?,
        lease.source_id.clone(),
        row.try_get("effective_from")?,
        row.try_get("effective_until")?,
        state,
        row.try_get::<String, _>("ownership_evidence_reference")?,
    )
    .map_err(|error| RefreshInputError::Corrupt(error.to_string()))?;
    if event.subscription_id.as_deref() != Some(allocation.subscription_id.as_str())
        || event.allocation_reference.as_deref()
            != Some(allocation.external_allocation_reference.as_str())
    {
        return Err(RefreshInputError::Corrupt(
            "receipt and allocation subscription identities differ".into(),
        ));
    }
    Ok(RefreshJobInputs {
        lease: lease.clone(),
        event,
        allocation,
        receipt_status: row.try_get("status")?,
    })
}

fn parse_payer_kind(value: &str) -> Result<PayerKind, RefreshInputError> {
    match value {
        "personal" => Ok(PayerKind::Personal),
        "sponsor" => Ok(PayerKind::Sponsor),
        _ => Err(RefreshInputError::Corrupt(format!(
            "unknown payer kind {value:?}"
        ))),
    }
}

fn parse_allocation_state(value: &str) -> Result<AllocationState, RefreshInputError> {
    match value {
        "pending" => Ok(AllocationState::Pending),
        "active" => Ok(AllocationState::Active),
        "ended" => Ok(AllocationState::Ended),
        _ => Err(RefreshInputError::Corrupt(format!(
            "unknown allocation state {value:?}"
        ))),
    }
}
