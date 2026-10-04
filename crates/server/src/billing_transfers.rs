//! Durable payer-transfer intents.
//!
//! A transfer changes who pays for one beneficiary without changing the beneficiary's account,
//! keys, membership, or founding identity. Provider execution is deliberately outside this module:
//! callbacks record each verified step here, so a timeout can be reconciled without issuing a new
//! financial identity. This is an internal, dormant seam until the payer-policy and provider
//! activation decisions are approved; declaring an intent does not start a provider checkout.

use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use thiserror::Error;
use uuid::Uuid;

use crate::billing_catalogue::BillingOffer;
use crate::error::Error;
use crate::founding_allocator;
use crate::org;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferPayer {
    Personal,
    Sponsor,
}

impl TransferPayer {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Personal => "personal",
            Self::Sponsor => "sponsor",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferState {
    Pending,
    DestinationPrepared,
    DestinationPaid,
    SourceAdjustmentPending,
    Completed,
    Failed,
    Unknown,
}

impl TransferState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::DestinationPrepared => "destination_prepared",
            Self::DestinationPaid => "destination_paid",
            Self::SourceAdjustmentPending => "source_adjustment_pending",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Unknown => "unknown",
        }
    }

    fn parse(value: &str) -> Result<Self, TransferError> {
        match value {
            "pending" => Ok(Self::Pending),
            "destination_prepared" => Ok(Self::DestinationPrepared),
            "destination_paid" => Ok(Self::DestinationPaid),
            "source_adjustment_pending" => Ok(Self::SourceAdjustmentPending),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "unknown" => Ok(Self::Unknown),
            _ => Err(TransferError::CorruptState),
        }
    }

    #[cfg(test)]
    fn is_live(self) -> bool {
        matches!(
            self,
            Self::Pending
                | Self::DestinationPrepared
                | Self::DestinationPaid
                | Self::SourceAdjustmentPending
                | Self::Unknown
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferRequest {
    pub actor_user_id: String,
    pub beneficiary_id: String,
    pub source_kind: TransferPayer,
    pub source_organization_id: Option<String>,
    pub destination_kind: TransferPayer,
    pub destination_organization_id: Option<String>,
    pub offer: BillingOffer,
    pub quote_version: i64,
    pub quote_expires_at_epoch: i64,
    pub effective_from: i64,
    pub effective_until: Option<i64>,
    pub idempotency_key: String,
}

impl TransferRequest {
    pub fn validate(&self, now_epoch: i64) -> Result<(), TransferError> {
        self.validate_shape()?;
        self.validate_timing(now_epoch)
    }

    fn validate_shape(&self) -> Result<(), TransferError> {
        for (value, field) in [
            (&self.actor_user_id, "actor_user_id"),
            (&self.beneficiary_id, "beneficiary_id"),
            (&self.idempotency_key, "idempotency_key"),
        ] {
            if value.trim().is_empty() {
                return Err(TransferError::InvalidField(field));
            }
        }
        if self.source_kind == self.destination_kind
            && self.source_organization_id == self.destination_organization_id
        {
            return Err(TransferError::InvalidField("payer boundary"));
        }
        if (self.source_kind == TransferPayer::Personal) != self.source_organization_id.is_none()
            || (self.destination_kind == TransferPayer::Personal)
                != self.destination_organization_id.is_none()
        {
            return Err(TransferError::InvalidField("payer organisation"));
        }
        if self.quote_version < 1 {
            return Err(TransferError::InvalidField("quote_version"));
        }
        if self.quote_expires_at_epoch <= 0 {
            return Err(TransferError::InvalidField("quote_expires_at_epoch"));
        }
        if self.effective_from < 0 || self.effective_from > self.quote_expires_at_epoch {
            return Err(TransferError::InvalidField("effective_from"));
        }
        Ok(())
    }

    fn validate_timing(&self, now_epoch: i64) -> Result<(), TransferError> {
        if self.quote_expires_at_epoch <= now_epoch {
            return Err(TransferError::QuoteExpired);
        }
        if self.effective_from < now_epoch {
            return Err(TransferError::InvalidField("effective_from"));
        }
        if self
            .effective_until
            .is_some_and(|until| until <= self.effective_from)
        {
            return Err(TransferError::InvalidField("effective_until"));
        }
        Ok(())
    }

    fn request_hash(&self) -> String {
        let quote_version = self.quote_version.to_string();
        let quote_expires_at = self.quote_expires_at_epoch.to_string();
        let effective_from = self.effective_from.to_string();
        let effective_until = self.effective_until.unwrap_or_default().to_string();
        let fields = [
            "sotto-transfer-v1",
            &self.actor_user_id,
            &self.beneficiary_id,
            self.source_kind.as_str(),
            self.source_organization_id.as_deref().unwrap_or(""),
            self.destination_kind.as_str(),
            self.destination_organization_id.as_deref().unwrap_or(""),
            self.offer.as_str(),
            &quote_version,
            &quote_expires_at,
            &effective_from,
            &effective_until,
            &self.idempotency_key,
        ];
        let mut input = Vec::new();
        for field in fields {
            input.extend_from_slice(&(field.len() as u64).to_be_bytes());
            input.extend_from_slice(field.as_bytes());
        }
        hex_digest(&Sha256::digest(input))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransferIntent {
    pub transfer_id: String,
    pub actor_user_id: String,
    pub beneficiary_id: String,
    pub source_kind: TransferPayer,
    pub source_organization_id: Option<String>,
    pub destination_kind: TransferPayer,
    pub destination_organization_id: Option<String>,
    pub offer: BillingOffer,
    pub request_hash: String,
    pub effective_from: i64,
    pub effective_until: Option<i64>,
    pub destination_operation_id: Option<String>,
    pub source_operation_id: Option<String>,
    pub destination_payment_reference: Option<String>,
    pub source_adjustment_reference: Option<String>,
    pub founding_award_id: Option<String>,
    pub state: TransferState,
    pub result_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginTransfer {
    Created(TransferIntent),
    AlreadyExists(TransferIntent),
}

#[derive(Debug, Error)]
pub enum TransferError {
    #[error("transfer has invalid {0}")]
    InvalidField(&'static str),
    #[error("transfer quote has expired")]
    QuoteExpired,
    #[error("transfer source is not active")]
    SourceInactive,
    #[error("transfer destination already covers this beneficiary")]
    DestinationAlreadyCovers,
    #[error("transfer is already in progress for this beneficiary")]
    AlreadyInProgress,
    #[error("transfer is not authorised")]
    Unauthorised,
    #[error("transfer state is corrupt")]
    CorruptState,
    #[error("transfer evidence conflicts with the stored result")]
    ResultConflict,
    #[error("founding award is owned by a different payer")]
    FoundingPayerConflict,
    #[error("transfer is not ready for this step")]
    InvalidTransition,
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("founding allocator error: {0}")]
    Founding(#[from] founding_allocator::FoundingAllocatorError),
}

impl From<TransferError> for Error {
    fn from(error: TransferError) -> Self {
        match error {
            TransferError::InvalidField(field) => {
                Self::BadRequest(format!("invalid transfer {field}"))
            }
            TransferError::QuoteExpired => Self::Conflict("transfer quote expired".into()),
            TransferError::SourceInactive => Self::Conflict("transfer source is not active".into()),
            TransferError::DestinationAlreadyCovers => {
                Self::Conflict("transfer destination already covers this beneficiary".into())
            }
            TransferError::AlreadyInProgress => {
                Self::Conflict("a transfer is already in progress for this beneficiary".into())
            }
            TransferError::Unauthorised => Self::Forbidden("transfer is not authorised".into()),
            TransferError::CorruptState => Self::Internal("transfer state is corrupt".into()),
            TransferError::ResultConflict
            | TransferError::FoundingPayerConflict
            | TransferError::InvalidTransition => {
                Self::Conflict("transfer state conflicts with stored evidence".into())
            }
            TransferError::Database(error) => Self::Db(error),
            TransferError::Founding(error) => Self::Internal(error.to_string()),
        }
    }
}

pub async fn begin_transfer(
    tx: &mut Transaction<'_, Postgres>,
    request: &TransferRequest,
    now_epoch: i64,
) -> Result<BeginTransfer, TransferError> {
    request.validate_shape()?;
    let beneficiary_exists: Option<String> =
        sqlx::query_scalar("SELECT id FROM users WHERE id = $1 FOR UPDATE")
            .bind(&request.beneficiary_id)
            .fetch_optional(&mut **tx)
            .await?;
    if beneficiary_exists.is_none() {
        return Err(TransferError::Unauthorised);
    }
    if (request.source_kind == TransferPayer::Personal
        || request.destination_kind == TransferPayer::Personal)
        && request.actor_user_id != request.beneficiary_id
    {
        return Err(TransferError::Unauthorised);
    }
    for organization_id in [
        request.source_organization_id.as_deref(),
        request.destination_organization_id.as_deref(),
    ]
    .into_iter()
    .flatten()
    .collect::<std::collections::BTreeSet<_>>()
    {
        let access = org::access_for_update(tx, organization_id, &request.actor_user_id)
            .await
            .map_err(|_| TransferError::Unauthorised)?;
        if !access.role().can_manage_members() {
            return Err(TransferError::Unauthorised);
        }
    }
    if let Some(existing) =
        load_by_idempotency(tx, &request.actor_user_id, &request.idempotency_key).await?
    {
        if existing.request_hash != request.request_hash() {
            return Err(TransferError::ResultConflict);
        }
        return Ok(BeginTransfer::AlreadyExists(existing));
    }
    request.validate_timing(now_epoch)?;
    let live: Option<String> = sqlx::query_scalar(
        "SELECT transfer_id FROM billing_transfer_intents WHERE beneficiary_id = $1 \
         AND state IN ('pending','destination_prepared','destination_paid','source_adjustment_pending','unknown') \
         FOR UPDATE",
    )
    .bind(&request.beneficiary_id)
    .fetch_optional(&mut **tx)
    .await?;
    if live.is_some() {
        return Err(TransferError::AlreadyInProgress);
    }
    ensure_source_active(tx, request).await?;
    ensure_destination_free(tx, request).await?;
    let founding_award_id: Option<String> = sqlx::query_scalar(
        "SELECT award_id FROM billing_founding_awards WHERE beneficiary_id = $1",
    )
    .bind(&request.beneficiary_id)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(award_id) = founding_award_id.as_deref() {
        let award = founding_allocator::load_award(tx, award_id)
            .await?
            .ok_or(TransferError::CorruptState)?;
        if award.payer_id != payer_id(request, request.source_kind)? {
            return Err(TransferError::FoundingPayerConflict);
        }
    }
    let transfer_id = format!("transfer:{}", Uuid::new_v4());
    let provider_key = format!(
        "sotto-transfer:{}",
        hex_digest(&Sha256::digest(transfer_id.as_bytes()))
    );
    sqlx::query(
        "INSERT INTO billing_transfer_intents \
         (transfer_id, actor_user_id, beneficiary_id, source_kind, source_organization_id, \
          destination_kind, destination_organization_id, offer, quote_version, quote_expires_at_epoch, \
          effective_from, effective_until, idempotency_key, request_hash, provider_idempotency_key, \
          founding_award_id) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)",
    )
    .bind(&transfer_id)
    .bind(&request.actor_user_id)
    .bind(&request.beneficiary_id)
    .bind(request.source_kind.as_str())
    .bind(request.source_organization_id.as_deref())
    .bind(request.destination_kind.as_str())
    .bind(request.destination_organization_id.as_deref())
    .bind(request.offer.as_str())
    .bind(request.quote_version)
    .bind(request.quote_expires_at_epoch)
    .bind(request.effective_from)
    .bind(request.effective_until)
    .bind(&request.idempotency_key)
    .bind(request.request_hash())
    .bind(provider_key)
    .bind(founding_award_id)
    .execute(&mut **tx)
    .await?;
    Ok(BeginTransfer::Created(
        load_transfer(tx, &transfer_id).await?,
    ))
}

pub async fn record_destination_prepared(
    tx: &mut Transaction<'_, Postgres>,
    transfer_id: &str,
    destination_operation_id: &str,
    provider_subscription_id: Option<&str>,
) -> Result<TransferIntent, TransferError> {
    if destination_operation_id.trim().is_empty() {
        return Err(TransferError::InvalidField("destination_operation_id"));
    }
    let updated = sqlx::query(
        "UPDATE billing_transfer_intents SET state = 'destination_prepared', \
         destination_operation_id = $2, destination_provider_subscription_id = $3, updated_at = now() \
         WHERE transfer_id = $1 AND state = 'pending' RETURNING transfer_id",
    )
    .bind(transfer_id)
    .bind(destination_operation_id)
    .bind(provider_subscription_id)
    .fetch_optional(&mut **tx)
    .await?;
    if updated.is_none() {
        let current = load_transfer(tx, transfer_id).await?;
        if current.state != TransferState::DestinationPrepared
            || current.destination_operation_id.as_deref() != Some(destination_operation_id)
        {
            return Err(TransferError::ResultConflict);
        }
    }
    load_transfer(tx, transfer_id).await
}

pub async fn record_destination_paid(
    tx: &mut Transaction<'_, Postgres>,
    transfer_id: &str,
    payment_reference: &str,
) -> Result<TransferIntent, TransferError> {
    if payment_reference.trim().is_empty() {
        return Err(TransferError::InvalidField("payment_reference"));
    }
    let updated = sqlx::query(
        "UPDATE billing_transfer_intents SET state = 'destination_paid', \
         destination_payment_reference = $2, updated_at = now() \
         WHERE transfer_id = $1 AND state IN ('destination_prepared','destination_paid') RETURNING transfer_id",
    )
    .bind(transfer_id)
    .bind(payment_reference)
    .fetch_optional(&mut **tx)
    .await?;
    if updated.is_none() {
        let current = load_transfer(tx, transfer_id).await?;
        if matches!(
            current.state,
            TransferState::DestinationPaid | TransferState::SourceAdjustmentPending
        ) && current.destination_payment_reference.as_deref() == Some(payment_reference)
        {
            return Ok(current);
        }
        return Err(TransferError::ResultConflict);
    }
    load_transfer(tx, transfer_id).await
}

/// Record that the source adjustment has been created after the destination payment is verified.
/// The provider call itself is outside this transaction; the operation id makes a retry safe.
pub async fn begin_source_adjustment(
    tx: &mut Transaction<'_, Postgres>,
    transfer_id: &str,
    source_operation_id: &str,
) -> Result<TransferIntent, TransferError> {
    if source_operation_id.trim().is_empty() {
        return Err(TransferError::InvalidField("source_operation_id"));
    }
    let updated = sqlx::query(
        "UPDATE billing_transfer_intents SET state = 'source_adjustment_pending', \
         source_operation_id = $2, updated_at = now() \
         WHERE transfer_id = $1 AND state = 'destination_paid' RETURNING transfer_id",
    )
    .bind(transfer_id)
    .bind(source_operation_id)
    .fetch_optional(&mut **tx)
    .await?;
    if updated.is_none() {
        let current = load_transfer(tx, transfer_id).await?;
        if current.state == TransferState::SourceAdjustmentPending
            && current.source_operation_id.as_deref() == Some(source_operation_id)
        {
            return Ok(current);
        }
        return Err(TransferError::ResultConflict);
    }
    load_transfer(tx, transfer_id).await
}

pub async fn complete_source_adjustment(
    tx: &mut Transaction<'_, Postgres>,
    transfer_id: &str,
    source_adjustment_reference: &str,
) -> Result<TransferIntent, TransferError> {
    if source_adjustment_reference.trim().is_empty() {
        return Err(TransferError::InvalidField("source_adjustment_reference"));
    }
    let transfer = load_transfer(tx, transfer_id).await?;
    if transfer.state == TransferState::Completed {
        if transfer.source_adjustment_reference.as_deref() == Some(source_adjustment_reference) {
            return Ok(transfer);
        }
        return Err(TransferError::ResultConflict);
    }
    if transfer.state != TransferState::SourceAdjustmentPending {
        return Err(TransferError::InvalidTransition);
    }
    if let Some(award_id) = transfer.founding_award_id.as_deref() {
        let destination_payer = match transfer.destination_kind {
            TransferPayer::Personal => transfer.beneficiary_id.as_str(),
            TransferPayer::Sponsor => transfer
                .destination_organization_id
                .as_deref()
                .ok_or(TransferError::CorruptState)?,
        };
        let award = founding_allocator::load_award(tx, award_id)
            .await?
            .ok_or(TransferError::CorruptState)?;
        if award.beneficiary_id != transfer.beneficiary_id {
            return Err(TransferError::CorruptState);
        }
        let source_payer = payer_id_for_transfer(
            transfer.source_kind,
            &transfer.beneficiary_id,
            transfer.source_organization_id.as_deref(),
        )?;
        if award.payer_id != source_payer && award.payer_id != destination_payer {
            return Err(TransferError::FoundingPayerConflict);
        }
        founding_allocator::transfer_payer(tx, award_id, destination_payer).await?;
    }
    sqlx::query(
        "UPDATE billing_transfer_intents SET state = 'completed', \
         source_adjustment_reference = $2, result_code = 'completed', updated_at = now() \
         WHERE transfer_id = $1 AND state = 'source_adjustment_pending'",
    )
    .bind(transfer_id)
    .bind(source_adjustment_reference)
    .execute(&mut **tx)
    .await?;
    load_transfer(tx, transfer_id).await
}

pub async fn mark_failed(
    tx: &mut Transaction<'_, Postgres>,
    transfer_id: &str,
    result_code: &str,
) -> Result<TransferIntent, TransferError> {
    if result_code.trim().is_empty() {
        return Err(TransferError::InvalidField("result_code"));
    }
    let updated = sqlx::query(
        "UPDATE billing_transfer_intents SET state = 'failed', result_code = $2, updated_at = now() \
         WHERE transfer_id = $1 AND state NOT IN ('completed','failed')",
    )
    .bind(transfer_id)
    .bind(result_code)
    .execute(&mut **tx)
    .await?;
    let current = load_transfer(tx, transfer_id).await?;
    if current.state == TransferState::Failed
        && current.result_code.as_deref() != Some(result_code)
        && updated.rows_affected() == 0
    {
        return Err(TransferError::ResultConflict);
    }
    Ok(current)
}

pub async fn load_for_actor(
    pool: &PgPool,
    transfer_id: &str,
    actor_user_id: &str,
) -> Result<Option<TransferIntent>, TransferError> {
    let row = sqlx::query(
        "SELECT * FROM billing_transfer_intents WHERE transfer_id = $1 AND actor_user_id = $2",
    )
    .bind(transfer_id)
    .bind(actor_user_id)
    .fetch_optional(pool)
    .await?;
    row.map(|row| transfer_from_row(&row)).transpose()
}

async fn ensure_source_active(
    tx: &mut Transaction<'_, Postgres>,
    request: &TransferRequest,
) -> Result<(), TransferError> {
    match request.source_kind {
        TransferPayer::Personal => {
            let active: Option<i32> = sqlx::query_scalar(
                "SELECT 1 FROM billing_personal_accounts \
                 WHERE user_id = $1 AND state IN ('active','past_due') \
                   AND stripe_subscription_id IS NOT NULL FOR UPDATE",
            )
            .bind(&request.beneficiary_id)
            .fetch_optional(&mut **tx)
            .await?;
            active.map(|_| ()).ok_or(TransferError::SourceInactive)
        }
        TransferPayer::Sponsor => {
            let organization_id = request
                .source_organization_id
                .as_deref()
                .ok_or(TransferError::CorruptState)?;
            let active: Option<i32> = sqlx::query_scalar(
                "SELECT 1 FROM billing_sponsored_seats \
                 WHERE organization_id = $1 AND beneficiary_id = $2 \
                   AND state = 'active' FOR UPDATE",
            )
            .bind(organization_id)
            .bind(&request.beneficiary_id)
            .fetch_one(&mut **tx)
            .await?;
            active.map(|_| ()).ok_or(TransferError::SourceInactive)
        }
    }
}

async fn ensure_destination_free(
    tx: &mut Transaction<'_, Postgres>,
    request: &TransferRequest,
) -> Result<(), TransferError> {
    let covered = match request.destination_kind {
        TransferPayer::Personal => {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM billing_personal_accounts \
             WHERE user_id = $1 AND state IN ('pending','active','past_due','refund_required'))",
            )
            .bind(&request.beneficiary_id)
            .fetch_one(&mut **tx)
            .await?
        }
        TransferPayer::Sponsor => {
            let organization_id = request
                .destination_organization_id
                .as_deref()
                .ok_or(TransferError::CorruptState)?;
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS(SELECT 1 FROM billing_sponsored_seats \
                 WHERE organization_id = $1 AND beneficiary_id = $2 \
                   AND state IN ('pending','active','scheduled_removal'))",
            )
            .bind(organization_id)
            .bind(&request.beneficiary_id)
            .fetch_one(&mut **tx)
            .await?
        }
    };
    (!covered)
        .then_some(())
        .ok_or(TransferError::DestinationAlreadyCovers)
}

async fn load_by_idempotency(
    tx: &mut Transaction<'_, Postgres>,
    actor_user_id: &str,
    idempotency_key: &str,
) -> Result<Option<TransferIntent>, TransferError> {
    let row = sqlx::query(
        "SELECT * FROM billing_transfer_intents WHERE actor_user_id = $1 AND idempotency_key = $2 FOR UPDATE",
    )
    .bind(actor_user_id)
    .bind(idempotency_key)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(|row| transfer_from_row(&row)).transpose()
}

async fn load_transfer(
    tx: &mut Transaction<'_, Postgres>,
    transfer_id: &str,
) -> Result<TransferIntent, TransferError> {
    let row =
        sqlx::query("SELECT * FROM billing_transfer_intents WHERE transfer_id = $1 FOR UPDATE")
            .bind(transfer_id)
            .fetch_one(&mut **tx)
            .await?;
    transfer_from_row(&row)
}

fn transfer_from_row(row: &sqlx::postgres::PgRow) -> Result<TransferIntent, TransferError> {
    Ok(TransferIntent {
        transfer_id: row.try_get("transfer_id")?,
        actor_user_id: row.try_get("actor_user_id")?,
        beneficiary_id: row.try_get("beneficiary_id")?,
        source_kind: parse_payer(&row.try_get::<String, _>("source_kind")?)?,
        source_organization_id: row.try_get("source_organization_id")?,
        destination_kind: parse_payer(&row.try_get::<String, _>("destination_kind")?)?,
        destination_organization_id: row.try_get("destination_organization_id")?,
        offer: parse_offer(&row.try_get::<String, _>("offer")?)?,
        request_hash: row.try_get("request_hash")?,
        effective_from: row.try_get("effective_from")?,
        effective_until: row.try_get("effective_until")?,
        destination_operation_id: row.try_get("destination_operation_id")?,
        source_operation_id: row.try_get("source_operation_id")?,
        destination_payment_reference: row.try_get("destination_payment_reference")?,
        source_adjustment_reference: row.try_get("source_adjustment_reference")?,
        founding_award_id: row.try_get("founding_award_id")?,
        state: TransferState::parse(&row.try_get::<String, _>("state")?)?,
        result_code: row.try_get("result_code")?,
    })
}

fn parse_payer(value: &str) -> Result<TransferPayer, TransferError> {
    match value {
        "personal" => Ok(TransferPayer::Personal),
        "sponsor" => Ok(TransferPayer::Sponsor),
        _ => Err(TransferError::CorruptState),
    }
}

fn parse_offer(value: &str) -> Result<BillingOffer, TransferError> {
    BillingOffer::ALL
        .into_iter()
        .find(|offer| offer.as_str() == value)
        .ok_or(TransferError::CorruptState)
}

fn payer_id(request: &TransferRequest, payer: TransferPayer) -> Result<&str, TransferError> {
    match payer {
        TransferPayer::Personal => Ok(&request.beneficiary_id),
        TransferPayer::Sponsor => request
            .source_organization_id
            .as_deref()
            .ok_or(TransferError::CorruptState),
    }
}

fn payer_id_for_transfer<'a>(
    payer: TransferPayer,
    beneficiary_id: &'a str,
    organization_id: Option<&'a str>,
) -> Result<&'a str, TransferError> {
    match payer {
        TransferPayer::Personal => Ok(beneficiary_id),
        TransferPayer::Sponsor => organization_id.ok_or(TransferError::CorruptState),
    }
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::{TransferPayer, TransferRequest, TransferState};
    use crate::billing_catalogue::BillingOffer;

    fn request() -> TransferRequest {
        TransferRequest {
            actor_user_id: "user".into(),
            beneficiary_id: "user".into(),
            source_kind: TransferPayer::Personal,
            source_organization_id: None,
            destination_kind: TransferPayer::Sponsor,
            destination_organization_id: Some("org".into()),
            offer: BillingOffer::StandardMonthly,
            quote_version: 1,
            quote_expires_at_epoch: 2_000,
            effective_from: 1_500,
            effective_until: None,
            idempotency_key: "key".into(),
        }
    }

    #[test]
    fn transfer_requires_a_distinct_well_shaped_payer_boundary() {
        assert!(request().validate(1_000).is_ok());
        let mut invalid = request();
        invalid.destination_organization_id = None;
        assert!(invalid.validate(1_000).is_err());
        let mut same = request();
        same.destination_kind = TransferPayer::Personal;
        assert!(same.validate(1_000).is_err());
    }

    #[test]
    fn transfer_expiry_and_effective_dates_fail_closed() {
        let mut invalid = request();
        invalid.quote_expires_at_epoch = 1_000;
        assert!(invalid.validate(1_000).is_err());
        let mut invalid = request();
        invalid.effective_from = 900;
        assert!(invalid.validate(1_000).is_err());
        let mut invalid = request();
        invalid.effective_from = 2_001;
        assert!(invalid.validate(1_000).is_err());
    }

    #[test]
    fn only_live_transfer_states_block_a_second_transfer() {
        assert!(TransferState::Pending.is_live());
        assert!(TransferState::SourceAdjustmentPending.is_live());
        assert!(!TransferState::Completed.is_live());
        assert!(!TransferState::Failed.is_live());
    }
}
