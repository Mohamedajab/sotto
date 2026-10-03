//! Durable, bounded storage for named sponsored Stripe allocation intervals.
//!
//! The provider normaliser consumes a complete [`SponsoredAllocationManifest`].  This module is
//! the persistence boundary for that manifest: an allocation is first registered by the provider
//! adapter, then its Stripe price is recorded once for the allocation's already-dated ownership
//! interval.  Replacing or removing a seat therefore requires a new allocation/source interval;
//! the old row is never silently rewritten.

use sqlx::{PgPool, Postgres, Row, Transaction};
use thiserror::Error;

use crate::cloud_provider::{PayerKind, ProviderContext, VerifiedAllocation};
use crate::cloud_provider_stripe::STRIPE_NAMESPACE;
use crate::cloud_provider_stripe_sponsored::{
    SponsoredAllocationInterval, SponsoredAllocationManifest,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SponsoredAllocationTermDisposition {
    Recorded,
    AlreadyRecorded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SponsoredAllocationTermReceipt {
    pub allocation_id: String,
    pub price_id: String,
    pub disposition: SponsoredAllocationTermDisposition,
}

#[derive(Debug, Error)]
pub enum SponsoredAllocationTermStoreError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("sponsored allocation terms require a Stripe sponsor allocation")]
    InvalidAllocation,
    #[error("sponsored allocation does not match its registered owner")]
    AllocationConflict,
    #[error("sponsored allocation has no registered owner")]
    AllocationMissing,
    #[error("sponsored allocation term conflicts with the registered term")]
    TermConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SponsoredManifestLoadLimits {
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl Default for SponsoredManifestLoadLimits {
    fn default() -> Self {
        Self {
            max_rows: 128,
            max_bytes: 256 * 1024,
        }
    }
}

impl SponsoredManifestLoadLimits {
    fn validate(self) -> Result<i64, SponsoredManifestLoadError> {
        if self.max_rows == 0 || self.max_bytes == 0 {
            return Err(SponsoredManifestLoadError::InvalidLimits);
        }
        self.max_rows
            .checked_add(1)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(SponsoredManifestLoadError::InvalidLimits)
    }
}

#[derive(Debug, Error)]
pub enum SponsoredManifestLoadError {
    #[error("sponsored manifest load limits must be nonzero and fit in a database integer")]
    InvalidLimits,
    #[error("sponsored allocation manifest exceeded the row bound of {limit}")]
    TooManyRows { limit: usize },
    #[error("sponsored allocation manifest exceeded the byte bound of {limit}")]
    BoundExceeded { limit: usize },
    #[error("sponsored allocation manifest is corrupt: {0}")]
    Corrupt(String),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Record the Stripe price for one already-registered sponsored allocation.
///
/// The caller owns the transaction and must commit it after this function succeeds.  The
/// operation is immutable: a replay of the same price and interval is idempotent, while any
/// changed price or boundary is rejected.  A seat replacement must use a new allocation row.
pub async fn record_sponsored_allocation_term(
    tx: &mut Transaction<'_, Postgres>,
    context: &ProviderContext,
    allocation: &VerifiedAllocation,
    price_id: &str,
) -> Result<SponsoredAllocationTermReceipt, SponsoredAllocationTermStoreError> {
    if context.namespace != STRIPE_NAMESPACE
        || allocation.payer_kind != PayerKind::Sponsor
        || price_id.trim().is_empty()
        || allocation.effective_from < 0
        || allocation
            .effective_until
            .is_some_and(|until| until <= allocation.effective_from)
    {
        return Err(SponsoredAllocationTermStoreError::InvalidAllocation);
    }

    let row = sqlx::query(
        "SELECT allocation.payer_id, allocation.payer_kind, allocation.beneficiary_id, \
                allocation.provider_namespace, allocation.provider_account_id, \
                allocation.provider_environment, allocation.provider_subscription_id, \
                allocation.provider_item_id, allocation.external_allocation_reference, \
                allocation.coverage_source_id, allocation.effective_from, allocation.effective_until, \
                allocation.state, allocation.ownership_evidence_reference, \
                payer.provider_namespace AS payer_namespace, payer.provider_account_id AS payer_account_id, \
                payer.provider_environment AS payer_environment, payer.provider_customer_id, \
                payer.payer_kind AS payer_payer_kind, \
                source.beneficiary_id AS source_beneficiary_id, source.provider_namespace AS source_namespace, \
                source.external_allocation_reference AS source_allocation_reference, \
                source.ownership_evidence_reference AS source_ownership_reference \
         FROM cloud_provider_allocations AS allocation \
         JOIN cloud_provider_payers AS payer ON payer.payer_id = allocation.payer_id \
         JOIN cloud_coverage_sources AS source ON source.source_id = allocation.coverage_source_id \
         WHERE allocation.allocation_id = $1 \
         FOR UPDATE OF allocation, payer, source",
    )
    .bind(&allocation.allocation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(SponsoredAllocationTermStoreError::AllocationMissing)?;

    if !allocation_row_matches(&row, context, allocation) {
        return Err(SponsoredAllocationTermStoreError::AllocationConflict);
    }

    let result = sqlx::query(
        "INSERT INTO cloud_provider_sponsored_allocation_terms \
         (allocation_id, price_id, effective_from, effective_until) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (allocation_id) DO NOTHING",
    )
    .bind(&allocation.allocation_id)
    .bind(price_id)
    .bind(allocation.effective_from)
    .bind(allocation.effective_until)
    .execute(&mut **tx)
    .await?;
    if result.rows_affected() == 1 {
        return Ok(SponsoredAllocationTermReceipt {
            allocation_id: allocation.allocation_id.clone(),
            price_id: price_id.to_owned(),
            disposition: SponsoredAllocationTermDisposition::Recorded,
        });
    }

    let existing = sqlx::query(
        "SELECT price_id, effective_from, effective_until \
         FROM cloud_provider_sponsored_allocation_terms WHERE allocation_id = $1",
    )
    .bind(&allocation.allocation_id)
    .fetch_optional(&mut **tx)
    .await?
    .ok_or(SponsoredAllocationTermStoreError::TermConflict)?;
    if existing.try_get::<String, _>("price_id")? == price_id
        && existing.try_get::<i64, _>("effective_from")? == allocation.effective_from
        && existing.try_get::<Option<i64>, _>("effective_until")? == allocation.effective_until
    {
        Ok(SponsoredAllocationTermReceipt {
            allocation_id: allocation.allocation_id.clone(),
            price_id: price_id.to_owned(),
            disposition: SponsoredAllocationTermDisposition::AlreadyRecorded,
        })
    } else {
        Err(SponsoredAllocationTermStoreError::TermConflict)
    }
}

/// Load a complete sponsored manifest in one repeatable-read snapshot.
///
/// Historical ended allocations remain in the manifest because a paid invoice may cover a term
/// that has already ended by the time it is replayed.  The row and byte limits are checked before
/// any manifest is returned; corruption or an over-bound history never yields a usable prefix.
pub async fn load_sponsored_allocation_manifest(
    pool: &PgPool,
    context: &ProviderContext,
    customer_id: &str,
    subscription_id: &str,
    limits: SponsoredManifestLoadLimits,
) -> Result<SponsoredAllocationManifest, SponsoredManifestLoadError> {
    let row_limit = limits.validate()?;
    if context.namespace != STRIPE_NAMESPACE
        || customer_id.trim().is_empty()
        || subscription_id.trim().is_empty()
    {
        return Err(SponsoredManifestLoadError::Corrupt(
            "manifest requires a Stripe context and nonempty customer and subscription".into(),
        ));
    }

    let mut tx = pool.begin().await?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await?;

    let missing_terms = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) \
         FROM cloud_provider_allocations AS allocation \
         JOIN cloud_provider_payers AS payer ON payer.payer_id = allocation.payer_id \
         LEFT JOIN cloud_provider_sponsored_allocation_terms AS term \
           ON term.allocation_id = allocation.allocation_id \
         WHERE allocation.provider_namespace = $1 \
           AND allocation.provider_account_id = $2 \
           AND allocation.provider_environment = $3 \
           AND allocation.provider_subscription_id = $4 \
           AND allocation.payer_kind = 'sponsor' \
           AND payer.provider_customer_id = $5 \
           AND payer.payer_kind = 'sponsor' \
           AND term.allocation_id IS NULL",
    )
    .bind(&context.namespace)
    .bind(&context.account_id)
    .bind(context.environment.as_str())
    .bind(subscription_id)
    .bind(customer_id)
    .fetch_one(&mut *tx)
    .await?;
    if missing_terms > 0 {
        return Err(SponsoredManifestLoadError::Corrupt(
            "registered sponsored allocation has no persisted term".into(),
        ));
    }

    let preflight = sqlx::query(
        "WITH candidates AS ( \
         SELECT allocation.allocation_id, allocation.beneficiary_id, allocation.provider_item_id, \
                allocation.external_allocation_reference, allocation.coverage_source_id, \
                allocation.effective_from, allocation.effective_until, allocation.state, \
                allocation.provider_namespace, allocation.provider_account_id, \
                allocation.provider_environment, allocation.provider_subscription_id, \
                allocation.ownership_evidence_reference, term.price_id, \
                payer.provider_namespace AS payer_namespace, payer.provider_account_id AS payer_account_id, \
                payer.provider_environment AS payer_environment, payer.provider_customer_id, payer.payer_kind, \
                source.provider_namespace AS source_namespace, \
                source.ownership_evidence_reference AS source_ownership_reference \
         FROM cloud_provider_sponsored_allocation_terms AS term \
         JOIN cloud_provider_allocations AS allocation ON allocation.allocation_id = term.allocation_id \
         JOIN cloud_provider_payers AS payer ON payer.payer_id = allocation.payer_id \
         JOIN cloud_coverage_sources AS source ON source.source_id = allocation.coverage_source_id \
         WHERE allocation.provider_namespace = $1 \
           AND allocation.provider_account_id = $2 \
           AND allocation.provider_environment = $3 \
           AND allocation.provider_subscription_id = $4 \
           AND payer.provider_customer_id = $5 \
           AND payer.payer_kind = 'sponsor' \
         ORDER BY allocation.effective_from, allocation.allocation_id COLLATE \"C\" \
         LIMIT $6 \
       ) \
       SELECT count(*)::BIGINT AS row_count, \
              COALESCE(SUM( \
                  octet_length(allocation_id) + octet_length(beneficiary_id) \
                + octet_length(provider_namespace) + octet_length(provider_account_id) \
                + octet_length(provider_environment) + octet_length(provider_subscription_id) \
                + octet_length(provider_item_id) + octet_length(external_allocation_reference) \
                + octet_length(coverage_source_id) + octet_length(ownership_evidence_reference) \
                + octet_length(state) + octet_length(price_id) \
                + octet_length(payer_namespace) + octet_length(payer_account_id) \
                + octet_length(payer_environment) + octet_length(provider_customer_id) \
                + octet_length(payer_kind) \
                + octet_length(source_namespace) + octet_length(source_ownership_reference) \
                + 2 * 8), 0)::TEXT AS manifest_bytes \
       FROM candidates",
    )
    .bind(&context.namespace)
    .bind(&context.account_id)
    .bind(context.environment.as_str())
    .bind(subscription_id)
    .bind(customer_id)
    .bind(row_limit)
    .fetch_one(&mut *tx)
    .await?;
    let row_count = preflight.try_get::<i64, _>("row_count")?;
    if row_count > limits.max_rows as i64 {
        return Err(SponsoredManifestLoadError::TooManyRows {
            limit: limits.max_rows,
        });
    }
    let bytes = preflight
        .try_get::<String, _>("manifest_bytes")?
        .parse::<u128>()
        .map_err(|_| SponsoredManifestLoadError::Corrupt("invalid manifest byte count".into()))?;
    if bytes > limits.max_bytes as u128 {
        return Err(SponsoredManifestLoadError::BoundExceeded {
            limit: limits.max_bytes,
        });
    }

    let rows = sqlx::query(
        "SELECT allocation.allocation_id, allocation.beneficiary_id, allocation.provider_namespace, \
                allocation.provider_account_id, allocation.provider_environment, \
                allocation.provider_subscription_id, allocation.provider_item_id, \
                allocation.external_allocation_reference, allocation.coverage_source_id, \
                allocation.effective_from, allocation.effective_until, allocation.state, \
                allocation.ownership_evidence_reference, allocation.payer_kind, \
                payer.provider_namespace AS payer_namespace, payer.provider_account_id AS payer_account_id, \
                payer.provider_environment AS payer_environment, payer.provider_customer_id, \
                payer.payer_kind AS payer_payer_kind, \
                term.price_id, term.effective_from AS term_effective_from, \
                term.effective_until AS term_effective_until, \
                source.beneficiary_id AS source_beneficiary_id, source.provider_namespace AS source_namespace, \
                source.external_allocation_reference AS source_allocation_reference, \
                source.ownership_evidence_reference AS source_ownership_reference \
         FROM cloud_provider_sponsored_allocation_terms AS term \
         JOIN cloud_provider_allocations AS allocation ON allocation.allocation_id = term.allocation_id \
         JOIN cloud_provider_payers AS payer ON payer.payer_id = allocation.payer_id \
         JOIN cloud_coverage_sources AS source ON source.source_id = allocation.coverage_source_id \
         WHERE allocation.provider_namespace = $1 \
           AND allocation.provider_account_id = $2 \
           AND allocation.provider_environment = $3 \
           AND allocation.provider_subscription_id = $4 \
           AND payer.provider_customer_id = $5 \
           AND payer.payer_kind = 'sponsor' \
         ORDER BY allocation.effective_from, allocation.allocation_id COLLATE \"C\" \
         LIMIT $6",
    )
    .bind(&context.namespace)
    .bind(&context.account_id)
    .bind(context.environment.as_str())
    .bind(subscription_id)
    .bind(customer_id)
    .bind(row_limit)
    .fetch_all(&mut *tx)
    .await?;
    if rows.len() > limits.max_rows {
        return Err(SponsoredManifestLoadError::TooManyRows {
            limit: limits.max_rows,
        });
    }

    let mut allocations = Vec::with_capacity(rows.len());
    for row in &rows {
        allocations.push(stored_interval(row, context, customer_id, subscription_id)?);
    }
    let manifest = SponsoredAllocationManifest::new(
        context.account_id.clone(),
        context.environment,
        customer_id.to_owned(),
        subscription_id.to_owned(),
        allocations,
    )
    .map_err(|error| SponsoredManifestLoadError::Corrupt(error.to_string()))?;
    tx.commit().await?;
    Ok(manifest)
}

fn allocation_row_matches(
    row: &sqlx::postgres::PgRow,
    context: &ProviderContext,
    allocation: &VerifiedAllocation,
) -> bool {
    row.try_get::<String, _>("payer_id").ok().as_deref() == Some(allocation.payer_id.as_str())
        && row.try_get::<String, _>("payer_kind").ok().as_deref() == Some("sponsor")
        && row.try_get::<String, _>("payer_payer_kind").ok().as_deref() == Some("sponsor")
        && row.try_get::<String, _>("beneficiary_id").ok().as_deref()
            == Some(allocation.beneficiary_id.as_str())
        && row
            .try_get::<String, _>("provider_namespace")
            .ok()
            .as_deref()
            == Some(context.namespace.as_str())
        && row
            .try_get::<String, _>("provider_account_id")
            .ok()
            .as_deref()
            == Some(context.account_id.as_str())
        && row
            .try_get::<String, _>("provider_environment")
            .ok()
            .as_deref()
            == Some(context.environment.as_str())
        && row
            .try_get::<String, _>("provider_customer_id")
            .ok()
            .as_deref()
            == Some(allocation.provider_customer_id.as_str())
        && row.try_get::<String, _>("payer_namespace").ok().as_deref()
            == Some(context.namespace.as_str())
        && row.try_get::<String, _>("payer_account_id").ok().as_deref()
            == Some(context.account_id.as_str())
        && row
            .try_get::<String, _>("payer_environment")
            .ok()
            .as_deref()
            == Some(context.environment.as_str())
        && row
            .try_get::<String, _>("provider_subscription_id")
            .ok()
            .as_deref()
            == Some(allocation.subscription_id.as_str())
        && row.try_get::<String, _>("provider_item_id").ok().as_deref()
            == Some(allocation.provider_item_id.as_str())
        && row
            .try_get::<String, _>("external_allocation_reference")
            .ok()
            .as_deref()
            == Some(allocation.external_allocation_reference.as_str())
        && row
            .try_get::<String, _>("coverage_source_id")
            .ok()
            .as_deref()
            == Some(allocation.source_id.as_str())
        && row.try_get::<i64, _>("effective_from").ok() == Some(allocation.effective_from)
        && row.try_get::<Option<i64>, _>("effective_until").ok() == Some(allocation.effective_until)
        && row.try_get::<String, _>("state").ok().as_deref() == Some(allocation_state(allocation))
        && row
            .try_get::<String, _>("ownership_evidence_reference")
            .ok()
            .as_deref()
            == Some(allocation.ownership_evidence_reference.as_str())
        && row
            .try_get::<String, _>("source_beneficiary_id")
            .ok()
            .as_deref()
            == Some(allocation.beneficiary_id.as_str())
        && row.try_get::<String, _>("source_namespace").ok().as_deref()
            == Some(context.namespace.as_str())
        && row
            .try_get::<String, _>("source_allocation_reference")
            .ok()
            .as_deref()
            == Some(allocation.external_allocation_reference.as_str())
        && row
            .try_get::<String, _>("source_ownership_reference")
            .ok()
            .as_deref()
            == Some(allocation.ownership_evidence_reference.as_str())
}

fn allocation_state(allocation: &VerifiedAllocation) -> &'static str {
    match allocation.state {
        crate::cloud_provider::AllocationState::Pending => "pending",
        crate::cloud_provider::AllocationState::Active => "active",
        crate::cloud_provider::AllocationState::Ended => "ended",
    }
}

fn stored_interval(
    row: &sqlx::postgres::PgRow,
    context: &ProviderContext,
    customer_id: &str,
    subscription_id: &str,
) -> Result<SponsoredAllocationInterval, SponsoredManifestLoadError> {
    let valid = row.try_get::<String, _>("payer_kind")? == "sponsor"
        && row.try_get::<String, _>("payer_payer_kind")? == "sponsor"
        && row.try_get::<String, _>("payer_namespace")? == context.namespace
        && row.try_get::<String, _>("payer_account_id")? == context.account_id
        && row.try_get::<String, _>("payer_environment")? == context.environment.as_str()
        && row.try_get::<String, _>("provider_namespace")? == context.namespace
        && row.try_get::<String, _>("provider_account_id")? == context.account_id
        && row.try_get::<String, _>("provider_environment")? == context.environment.as_str()
        && row.try_get::<String, _>("provider_subscription_id")? == subscription_id
        && row.try_get::<String, _>("provider_customer_id")? == customer_id
        && row.try_get::<String, _>("source_namespace")? == context.namespace
        && row.try_get::<String, _>("source_beneficiary_id")?
            == row.try_get::<String, _>("beneficiary_id")?
        && row.try_get::<String, _>("source_allocation_reference")?
            == row.try_get::<String, _>("external_allocation_reference")?
        && row.try_get::<String, _>("source_ownership_reference")?
            == row.try_get::<String, _>("ownership_evidence_reference")?
        && row.try_get::<i64, _>("term_effective_from")?
            == row.try_get::<i64, _>("effective_from")?
        && row.try_get::<Option<i64>, _>("term_effective_until")?
            == row.try_get::<Option<i64>, _>("effective_until")?;
    if !valid {
        return Err(SponsoredManifestLoadError::Corrupt(
            "stored sponsored allocation owner or term does not match".into(),
        ));
    }
    SponsoredAllocationInterval::new(
        row.try_get::<String, _>("coverage_source_id")?,
        row.try_get::<String, _>("external_allocation_reference")?,
        row.try_get::<String, _>("beneficiary_id")?,
        row.try_get::<String, _>("provider_item_id")?,
        row.try_get::<String, _>("price_id")?,
        row.try_get::<i64, _>("effective_from")?,
        row.try_get::<Option<i64>, _>("effective_until")?,
    )
    .map_err(|error| SponsoredManifestLoadError::Corrupt(error.to_string()))
}
