//! Durable identity and recovery state for hosted billing operations.
//!
//! This module deliberately stops at the provider boundary. A caller must persist an operation
//! before making a Stripe request, then record a provider result using the same operation id. An
//! unknown result remains reconcilable and can never be retried under a fresh financial identity.

use std::time::{SystemTime, UNIX_EPOCH};

use sqlx::{PgPool, Postgres, Row, Transaction};
use thiserror::Error;

use crate::billing_catalogue::BillingOffer;
use crate::error::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BillingOperationState {
    Pending,
    Succeeded,
    Failed,
    Unknown,
}

impl BillingOperationState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }

    fn parse(value: &str) -> Result<Self, BillingOperationError> {
        match value {
            "pending" => Ok(Self::Pending),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "unknown" => Ok(Self::Unknown),
            _ => Err(BillingOperationError::CorruptState),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BillingOperationRequest {
    pub operation_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub actor_user_id: String,
    pub payer_id: String,
    pub beneficiary_id: String,
    pub offer: BillingOffer,
    pub quote_version: i64,
    pub quote_expires_at_epoch: i64,
    pub provider_idempotency_key: String,
}

impl BillingOperationRequest {
    pub fn validate(&self, now_epoch: i64) -> Result<(), BillingOperationError> {
        for (value, field) in [
            (&self.operation_id, "operation_id"),
            (&self.idempotency_key, "idempotency_key"),
            (&self.request_hash, "request_hash"),
            (&self.actor_user_id, "actor_user_id"),
            (&self.payer_id, "payer_id"),
            (&self.beneficiary_id, "beneficiary_id"),
            (&self.provider_idempotency_key, "provider_idempotency_key"),
        ] {
            if value.trim().is_empty() {
                return Err(BillingOperationError::InvalidField(field));
            }
        }
        if self.quote_version < 1 {
            return Err(BillingOperationError::InvalidField("quote_version"));
        }
        if self.quote_expires_at_epoch <= now_epoch {
            return Err(BillingOperationError::QuoteExpired);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BillingOperation {
    pub operation_id: String,
    pub idempotency_key: String,
    pub request_hash: String,
    pub actor_user_id: String,
    pub payer_id: String,
    pub beneficiary_id: String,
    pub offer: String,
    pub quote_version: i64,
    pub quote_expires_at_epoch: i64,
    pub provider_idempotency_key: String,
    pub provider_operation_id: Option<String>,
    pub state: BillingOperationState,
    pub result_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginOperation {
    Created(BillingOperation),
    AlreadyExists(BillingOperation),
}

#[derive(Debug, Error)]
pub enum BillingOperationError {
    #[error("billing operation has invalid {0}")]
    InvalidField(&'static str),
    #[error("billing quote has expired")]
    QuoteExpired,
    #[error("billing idempotency key conflicts with a different request")]
    IdempotencyConflict,
    #[error("billing operation is not pending")]
    NotPending,
    #[error("billing operation state is corrupt")]
    CorruptState,
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

impl From<BillingOperationError> for Error {
    fn from(error: BillingOperationError) -> Self {
        match error {
            BillingOperationError::InvalidField(field) => {
                Self::BadRequest(format!("invalid billing operation {field}"))
            }
            BillingOperationError::QuoteExpired => Self::Conflict("billing quote expired".into()),
            BillingOperationError::IdempotencyConflict => {
                Self::Conflict("billing idempotency key conflicts with a different request".into())
            }
            BillingOperationError::NotPending => {
                Self::Conflict("billing operation is not pending".into())
            }
            BillingOperationError::CorruptState => {
                Self::Internal("billing operation state is corrupt".into())
            }
            BillingOperationError::Database(error) => Self::Db(error),
        }
    }
}

/// Persist an operation identity before provider I/O. The caller may pass a transaction that also
/// holds payer/beneficiary authorisation locks; no provider call belongs inside this transaction.
pub async fn begin_operation(
    tx: &mut Transaction<'_, Postgres>,
    request: &BillingOperationRequest,
) -> Result<BeginOperation, BillingOperationError> {
    request.validate(current_epoch())?;
    let inserted = sqlx::query(
        "INSERT INTO billing_operations (operation_id, idempotency_key, request_hash, actor_user_id, \
         payer_id, beneficiary_id, offer, quote_version, quote_expires_at_epoch, \
         provider_idempotency_key) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) \
         ON CONFLICT (actor_user_id, idempotency_key) DO NOTHING RETURNING operation_id",
    )
    .bind(&request.operation_id)
    .bind(&request.idempotency_key)
    .bind(&request.request_hash)
    .bind(&request.actor_user_id)
    .bind(&request.payer_id)
    .bind(&request.beneficiary_id)
    .bind(request.offer.as_str())
    .bind(request.quote_version)
    .bind(request.quote_expires_at_epoch)
    .bind(&request.provider_idempotency_key)
    .fetch_optional(&mut **tx)
    .await?;

    if inserted.is_some() {
        return Ok(BeginOperation::Created(
            load_operation(tx, &request.operation_id).await?,
        ));
    }
    let existing = load_by_idempotency(tx, &request.actor_user_id, &request.idempotency_key)
        .await?
        .ok_or(BillingOperationError::CorruptState)?;
    if existing.request_hash != request.request_hash
        || existing.offer != request.offer.as_str()
        || existing.quote_version != request.quote_version
        || existing.payer_id != request.payer_id
        || existing.beneficiary_id != request.beneficiary_id
    {
        return Err(BillingOperationError::IdempotencyConflict);
    }
    Ok(BeginOperation::AlreadyExists(existing))
}

/// Record a provider outcome using the persisted operation identity. Unknown outcomes remain
/// explicitly recoverable and never become a fresh operation.
pub async fn record_provider_result(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: &str,
    state: BillingOperationState,
    provider_operation_id: Option<&str>,
    result_code: Option<&str>,
) -> Result<BillingOperation, BillingOperationError> {
    if !matches!(
        state,
        BillingOperationState::Succeeded
            | BillingOperationState::Failed
            | BillingOperationState::Unknown
    ) {
        return Err(BillingOperationError::InvalidField("provider result state"));
    }
    let result = sqlx::query(
        "UPDATE billing_operations SET state = $2, provider_operation_id = COALESCE($3, provider_operation_id), \
         result_code = $4, updated_at = now() WHERE operation_id = $1 AND state IN ('pending','unknown') \
         RETURNING operation_id",
    )
    .bind(operation_id)
    .bind(state.as_str())
    .bind(provider_operation_id)
    .bind(result_code)
    .fetch_optional(&mut **tx)
    .await?;
    if result.is_none() {
        let existing = load_operation(tx, operation_id).await?;
        if !matches!(
            existing.state,
            BillingOperationState::Succeeded
                | BillingOperationState::Failed
                | BillingOperationState::Unknown
        ) {
            return Err(BillingOperationError::NotPending);
        }
    }
    load_operation(tx, operation_id).await
}

pub async fn load_reconciliation_candidates(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<BillingOperation>, BillingOperationError> {
    if !(1..=1_000).contains(&limit) {
        return Err(BillingOperationError::InvalidField("limit"));
    }
    let rows = sqlx::query(
        "SELECT operation_id, idempotency_key, request_hash, actor_user_id, payer_id, \
         beneficiary_id, offer, quote_version, quote_expires_at_epoch, provider_idempotency_key, \
         provider_operation_id, state, result_code FROM billing_operations \
         WHERE state IN ('pending','unknown') ORDER BY updated_at, operation_id LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.into_iter().map(operation_from_row).collect()
}

async fn load_operation(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: &str,
) -> Result<BillingOperation, BillingOperationError> {
    let row = sqlx::query(
        "SELECT operation_id, idempotency_key, request_hash, actor_user_id, payer_id, \
         beneficiary_id, offer, quote_version, quote_expires_at_epoch, provider_idempotency_key, \
         provider_operation_id, state, result_code FROM billing_operations WHERE operation_id = $1",
    )
    .bind(operation_id)
    .fetch_one(&mut **tx)
    .await?;
    operation_from_row(row)
}

async fn load_by_idempotency(
    tx: &mut Transaction<'_, Postgres>,
    actor_user_id: &str,
    idempotency_key: &str,
) -> Result<Option<BillingOperation>, BillingOperationError> {
    let row = sqlx::query(
        "SELECT operation_id, idempotency_key, request_hash, actor_user_id, payer_id, \
         beneficiary_id, offer, quote_version, quote_expires_at_epoch, provider_idempotency_key, \
         provider_operation_id, state, result_code FROM billing_operations \
         WHERE actor_user_id = $1 AND idempotency_key = $2",
    )
    .bind(actor_user_id)
    .bind(idempotency_key)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(operation_from_row).transpose()
}

fn operation_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<BillingOperation, BillingOperationError> {
    Ok(BillingOperation {
        operation_id: row.try_get("operation_id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        request_hash: row.try_get("request_hash")?,
        actor_user_id: row.try_get("actor_user_id")?,
        payer_id: row.try_get("payer_id")?,
        beneficiary_id: row.try_get("beneficiary_id")?,
        offer: row.try_get("offer")?,
        quote_version: row.try_get("quote_version")?,
        quote_expires_at_epoch: row.try_get("quote_expires_at_epoch")?,
        provider_idempotency_key: row.try_get("provider_idempotency_key")?,
        provider_operation_id: row.try_get("provider_operation_id")?,
        state: BillingOperationState::parse(row.try_get::<String, _>("state")?.as_str())?,
        result_code: row.try_get("result_code")?,
    })
}

fn current_epoch() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> BillingOperationRequest {
        BillingOperationRequest {
            operation_id: "op_1".into(),
            idempotency_key: "idem_1".into(),
            request_hash: "hash_1".into(),
            actor_user_id: "user_1".into(),
            payer_id: "payer_1".into(),
            beneficiary_id: "person_1".into(),
            offer: BillingOffer::StandardMonthly,
            quote_version: 1,
            quote_expires_at_epoch: 2_000_000_000,
            provider_idempotency_key: "stripe-op-1".into(),
        }
    }

    #[test]
    fn request_requires_unexpired_quote_and_non_empty_identity() {
        let mut request = request();
        request.validate(1_000_000_000).unwrap();
        request.quote_expires_at_epoch = 1_000_000_000;
        assert!(matches!(
            request.validate(1_000_000_000),
            Err(BillingOperationError::QuoteExpired)
        ));
        request.quote_expires_at_epoch = 2_000_000_000;
        request.idempotency_key.clear();
        assert!(matches!(
            request.validate(1_000_000_000),
            Err(BillingOperationError::InvalidField("idempotency_key"))
        ));
    }

    #[test]
    fn state_values_are_closed_and_unknown_provider_results_remain_recoverable() {
        assert_eq!(
            BillingOperationState::parse("pending").unwrap(),
            BillingOperationState::Pending
        );
        assert_eq!(
            BillingOperationState::parse("unknown").unwrap(),
            BillingOperationState::Unknown
        );
        assert!(matches!(
            BillingOperationState::parse("paid"),
            Err(BillingOperationError::CorruptState)
        ));
    }
}
