//! Caller-owned persistence for verified personal Stripe renewal failures.
//!
//! This module accepts only the sealed result of the signed renewal decoder. It records the
//! generic receipt, accepts the #471 invalidation fence, and stores the Stripe-specific
//! predecessor relationship in one transaction. The caller must commit only after this function
//! returns successfully; every error requires rollback.

use sqlx::{Postgres, Row, Transaction};
use thiserror::Error;

use crate::cloud_provider::{
    accept_provider_invalidation, record_verified_event, InvalidationDisposition, PayerKind,
    ProviderAdapterError, ProviderContext, VerifiedAllocation,
};
use crate::cloud_provider_stripe::STRIPE_NAMESPACE;
use crate::cloud_provider_stripe_renewals::StripeRenewalFailureEvidence;

/// Whether this event advanced the beneficiary fence or replayed an existing acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StripeRenewalFailureDisposition {
    Accepted,
    AlreadyAccepted,
}

/// The durable identity returned after a renewal failure is accepted or replayed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StripeRenewalFailureAcceptance {
    pub event_id: String,
    pub evidence_reference: String,
    pub accepted_generation: i64,
    pub disposition: StripeRenewalFailureDisposition,
}

#[derive(Debug, Error)]
pub enum StripeRenewalFailureStoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error(transparent)]
    Provider(#[from] ProviderAdapterError),
    #[error("stored Stripe renewal evidence conflicts with the accepted event")]
    EvidenceConflict,
    #[error("accepted Stripe invalidation has no renewal evidence row")]
    EvidenceMissing,
}

/// Atomically persist one decoder-linked renewal failure and its provider invalidation.
///
/// The transaction belongs to the caller. A successful return does not commit it; callers must
/// commit explicitly. If this function returns an error after an event or association was written,
/// the caller must roll the transaction back rather than attempting a partial recovery.
pub async fn accept_personal_renewal_failure(
    tx: &mut Transaction<'_, Postgres>,
    context: &ProviderContext,
    allocation: &VerifiedAllocation,
    evidence: &StripeRenewalFailureEvidence,
) -> Result<StripeRenewalFailureAcceptance, StripeRenewalFailureStoreError> {
    validate_evidence_context(context, allocation, evidence)?;
    let event = evidence.verified_event()?;
    let _event_disposition = record_verified_event(tx, context, &event).await?;
    let invalidation = accept_provider_invalidation(tx, context, &event, allocation).await?;
    let accepted_generation = match invalidation {
        InvalidationDisposition::Accepted { generation }
        | InvalidationDisposition::AlreadyAccepted { generation } => generation,
    };
    let disposition = match invalidation {
        InvalidationDisposition::Accepted { .. } => StripeRenewalFailureDisposition::Accepted,
        InvalidationDisposition::AlreadyAccepted { .. } => {
            StripeRenewalFailureDisposition::AlreadyAccepted
        }
    };

    let inserted = sqlx::query(
        "INSERT INTO cloud_provider_stripe_renewal_failures \
         (provider_namespace, provider_account_id, provider_environment, event_id, \
          evidence_version, evidence_reference, renewal_id, invoice_id, invoice_line_id, \
          predecessor_invoice_id, predecessor_evidence_reference, provider_customer_id, \
          subscription_id, provider_item_id, allocation_reference, beneficiary_id, allocation_id, \
          coverage_source_id, predecessor_period_start, predecessor_period_end, \
          renewal_period_start, renewal_period_end, event_created_at, interval, accepted_generation) \
         VALUES ('stripe', $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, \
                 $16, $17, $18, $19, $20, $21, $22, $23, $24)",
    )
    .bind(evidence.provider_account_id())
    .bind(evidence.environment().as_str())
    .bind(evidence.event_id())
    .bind(crate::cloud_provider_stripe_renewals::RENEWAL_EVIDENCE_VERSION)
    .bind(evidence.evidence_reference())
    .bind(evidence.renewal_id())
    .bind(evidence.invoice_id())
    .bind(evidence.invoice_line_id())
    .bind(evidence.predecessor_invoice_id())
    .bind(evidence.predecessor_evidence_reference())
    .bind(evidence.customer_id())
    .bind(evidence.subscription_id())
    .bind(evidence.provider_item_id())
    .bind(evidence.allocation_reference())
    .bind(&allocation.beneficiary_id)
    .bind(&allocation.allocation_id)
    .bind(&allocation.source_id)
    .bind(evidence.predecessor_period_start())
    .bind(evidence.predecessor_period_end())
    .bind(evidence.renewal_period_start())
    .bind(evidence.renewal_period_end())
    .bind(evidence.event_created_at())
    .bind(evidence.interval().to_string())
    .bind(accepted_generation)
    .execute(&mut **tx)
    .await;

    match inserted {
        Ok(result) if result.rows_affected() == 1 => Ok(StripeRenewalFailureAcceptance {
            event_id: evidence.event_id().to_owned(),
            evidence_reference: evidence.evidence_reference().to_owned(),
            accepted_generation,
            disposition,
        }),
        Ok(_) => {
            compare_existing(
                tx,
                context,
                allocation,
                evidence,
                accepted_generation,
                disposition,
            )
            .await
        }
        Err(sqlx::Error::Database(database)) if database.code().as_deref() == Some("23505") => {
            compare_existing(
                tx,
                context,
                allocation,
                evidence,
                accepted_generation,
                disposition,
            )
            .await
        }
        Err(error) => Err(ProviderAdapterError::Database(error).into()),
    }
}

fn validate_evidence_context(
    context: &ProviderContext,
    allocation: &VerifiedAllocation,
    evidence: &StripeRenewalFailureEvidence,
) -> Result<(), ProviderAdapterError> {
    if context.namespace != STRIPE_NAMESPACE
        || allocation.payer_kind != PayerKind::Personal
        || evidence.provider_account_id() != context.account_id
        || evidence.environment() != context.environment
        || allocation.provider_customer_id != evidence.customer_id()
        || allocation.subscription_id != evidence.subscription_id()
        || allocation.provider_item_id != evidence.provider_item_id()
        || allocation.external_allocation_reference != evidence.allocation_reference()
    {
        return Err(ProviderAdapterError::ProviderContextMismatch);
    }
    Ok(())
}

async fn compare_existing(
    tx: &mut Transaction<'_, Postgres>,
    context: &ProviderContext,
    allocation: &VerifiedAllocation,
    evidence: &StripeRenewalFailureEvidence,
    accepted_generation: i64,
    disposition: StripeRenewalFailureDisposition,
) -> Result<StripeRenewalFailureAcceptance, StripeRenewalFailureStoreError> {
    let row = sqlx::query(
        "SELECT evidence_version, evidence_reference, renewal_id, invoice_id, invoice_line_id, \
                predecessor_invoice_id, predecessor_evidence_reference, provider_customer_id, \
                subscription_id, provider_item_id, allocation_reference, beneficiary_id, \
                allocation_id, coverage_source_id, predecessor_period_start, predecessor_period_end, \
                renewal_period_start, renewal_period_end, event_created_at, interval, \
                accepted_generation \
         FROM cloud_provider_stripe_renewal_failures \
         WHERE provider_namespace = 'stripe' AND provider_account_id = $1 \
           AND provider_environment = $2 AND event_id = $3",
    )
    .bind(&context.account_id)
    .bind(context.environment.as_str())
    .bind(evidence.event_id())
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(StripeRenewalFailureStoreError::EvidenceMissing)?;

    let same = row.try_get::<i16, _>("evidence_version")?
        == crate::cloud_provider_stripe_renewals::RENEWAL_EVIDENCE_VERSION
        && row.try_get::<String, _>("evidence_reference")? == evidence.evidence_reference()
        && row.try_get::<String, _>("renewal_id")? == evidence.renewal_id()
        && row.try_get::<String, _>("invoice_id")? == evidence.invoice_id()
        && row.try_get::<String, _>("invoice_line_id")? == evidence.invoice_line_id()
        && row.try_get::<String, _>("predecessor_invoice_id")? == evidence.predecessor_invoice_id()
        && row.try_get::<String, _>("predecessor_evidence_reference")?
            == evidence.predecessor_evidence_reference()
        && row.try_get::<String, _>("provider_customer_id")? == evidence.customer_id()
        && row.try_get::<String, _>("subscription_id")? == evidence.subscription_id()
        && row.try_get::<String, _>("provider_item_id")? == evidence.provider_item_id()
        && row.try_get::<String, _>("allocation_reference")? == evidence.allocation_reference()
        && row.try_get::<String, _>("beneficiary_id")? == allocation.beneficiary_id
        && row.try_get::<String, _>("allocation_id")? == allocation.allocation_id
        && row.try_get::<String, _>("coverage_source_id")? == allocation.source_id
        && row.try_get::<i64, _>("predecessor_period_start")?
            == evidence.predecessor_period_start()
        && row.try_get::<i64, _>("predecessor_period_end")? == evidence.predecessor_period_end()
        && row.try_get::<i64, _>("renewal_period_start")? == evidence.renewal_period_start()
        && row.try_get::<i64, _>("renewal_period_end")? == evidence.renewal_period_end()
        && row.try_get::<i64, _>("event_created_at")? == evidence.event_created_at()
        && row.try_get::<String, _>("interval")? == evidence.interval().to_string()
        && row.try_get::<i64, _>("accepted_generation")? == accepted_generation;
    if !same {
        return Err(StripeRenewalFailureStoreError::EvidenceConflict);
    }
    Ok(StripeRenewalFailureAcceptance {
        event_id: evidence.event_id().to_owned(),
        evidence_reference: evidence.evidence_reference().to_owned(),
        accepted_generation,
        disposition,
    })
}
