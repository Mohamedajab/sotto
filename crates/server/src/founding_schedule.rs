//! Durable retries for Stripe schedules that enact founding-price end dates.
//!
//! The award is the customer contract; this queue records whether Stripe accepted its future
//! phase. Provider I/O always happens after the claim transaction commits.

use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::billing::{ProviderError, ProviderErrorKind};
use crate::founding_allocator::{FoundingAward, FoundingPayerKind};

const LEASE_SECONDS: i64 = 120;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundingScheduleJob {
    pub job_id: String,
    pub award_id: String,
    pub beneficiary_id: String,
    pub payer_kind: FoundingPayerKind,
    pub payer_id: String,
    pub subscription_id: String,
    pub idempotency_key: String,
    pub lease_token: String,
    pub attempt_count: i32,
}

pub async fn enqueue(
    tx: &mut Transaction<'_, Postgres>,
    award: &FoundingAward,
    subscription_id: &str,
) -> Result<(), sqlx::Error> {
    if subscription_id.trim().is_empty() {
        return Err(sqlx::Error::Protocol(
            "empty founding subscription id".into(),
        ));
    }
    let job_id = format!("founding-schedule:{}", award.award_id);
    let idempotency_key = format!("founding-transition:{}:1", award.award_id);
    sqlx::query(
        "INSERT INTO billing_founding_schedule_jobs \
         (job_id, award_id, beneficiary_id, payer_kind, payer_id, provider_subscription_id, \
          provider_idempotency_key) \
         VALUES ($1,$2,$3,$4,$5,$6,$7) \
         ON CONFLICT (award_id) DO UPDATE SET \
           beneficiary_id = EXCLUDED.beneficiary_id, payer_kind = EXCLUDED.payer_kind, \
           payer_id = EXCLUDED.payer_id, provider_subscription_id = EXCLUDED.provider_subscription_id, \
           state = 'pending', provider_schedule_id = NULL, attempt_count = 0, last_error_code = NULL, \
           next_attempt_at = now(), \
           version = billing_founding_schedule_jobs.version + 1, \
           provider_idempotency_key = 'founding-transition:' || billing_founding_schedule_jobs.award_id \
             || ':' || (billing_founding_schedule_jobs.version + 1)::text, \
           lease_token = NULL, lease_until = NULL, updated_at = now() \
         WHERE billing_founding_schedule_jobs.beneficiary_id IS DISTINCT FROM EXCLUDED.beneficiary_id \
            OR billing_founding_schedule_jobs.payer_kind IS DISTINCT FROM EXCLUDED.payer_kind \
            OR billing_founding_schedule_jobs.payer_id IS DISTINCT FROM EXCLUDED.payer_id \
            OR billing_founding_schedule_jobs.provider_subscription_id IS DISTINCT FROM EXCLUDED.provider_subscription_id",
    )
    .bind(job_id)
    .bind(&award.award_id)
    .bind(&award.beneficiary_id)
    .bind(award.payer_kind.as_str())
    .bind(&award.payer_id)
    .bind(subscription_id)
    .bind(idempotency_key)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn mark_invoice_unresolved(
    tx: &mut Transaction<'_, Postgres>,
    award_id: &str,
    error_code: &'static str,
) -> Result<(), sqlx::Error> {
    if !matches!(
        error_code,
        "invoice_price_mismatch" | "invoice_boundary_missing"
    ) {
        return Err(sqlx::Error::Protocol(
            "invalid founding invoice transition error code".into(),
        ));
    }
    sqlx::query(
        "UPDATE billing_founding_schedule_jobs SET state = 'unresolved', \
           provider_schedule_id = NULL, last_error_code = $2, lease_token = NULL, \
           lease_until = NULL, next_attempt_at = now(), version = version + 1, \
           provider_idempotency_key = 'founding-transition:' || award_id || ':' || (version + 1)::text, \
           updated_at = now() \
         WHERE award_id = $1 AND state <> 'not_required' \
           AND (state <> 'unresolved' OR last_error_code IS DISTINCT FROM $2)",
    )
    .bind(award_id)
    .bind(error_code)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn claim_due(pool: &PgPool) -> Result<Option<FoundingScheduleJob>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let row = sqlx::query(
        "SELECT job_id, award_id, beneficiary_id, payer_kind, payer_id, provider_subscription_id, \
                provider_idempotency_key, attempt_count \
         FROM billing_founding_schedule_jobs \
         WHERE state IN ('pending', 'unresolved') \
           AND next_attempt_at <= now() \
           AND (lease_until IS NULL OR lease_until <= now()) \
         ORDER BY created_at, job_id LIMIT 1 FOR UPDATE SKIP LOCKED",
    )
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        tx.commit().await?;
        return Ok(None);
    };
    let job_id: String = row.try_get("job_id")?;
    let lease_token = Uuid::new_v4().to_string();
    let row = sqlx::query(
        "UPDATE billing_founding_schedule_jobs SET lease_token = $2, \
           lease_until = now() + ($3 * interval '1 second'), attempt_count = attempt_count + 1, \
           updated_at = now() \
         WHERE job_id = $1 RETURNING job_id, award_id, beneficiary_id, payer_kind, payer_id, \
           provider_subscription_id, provider_idempotency_key, attempt_count",
    )
    .bind(&job_id)
    .bind(&lease_token)
    .bind(LEASE_SECONDS)
    .fetch_one(&mut *tx)
    .await?;
    let job = FoundingScheduleJob {
        job_id: row.try_get("job_id")?,
        award_id: row.try_get("award_id")?,
        beneficiary_id: row.try_get("beneficiary_id")?,
        payer_kind: match row.try_get::<String, _>("payer_kind")?.as_str() {
            "personal" => FoundingPayerKind::Personal,
            "sponsor" => FoundingPayerKind::Sponsor,
            _ => return Err(sqlx::Error::Protocol("invalid founding payer kind".into())),
        },
        payer_id: row.try_get("payer_id")?,
        subscription_id: row.try_get("provider_subscription_id")?,
        idempotency_key: row.try_get("provider_idempotency_key")?,
        lease_token,
        attempt_count: row.try_get("attempt_count")?,
    };
    tx.commit().await?;
    Ok(Some(job))
}

pub async fn mark_scheduled(
    pool: &PgPool,
    job: &FoundingScheduleJob,
    schedule_id: &str,
) -> Result<bool, sqlx::Error> {
    let changed = sqlx::query(
        "UPDATE billing_founding_schedule_jobs SET state = 'scheduled', provider_schedule_id = $3, \
           last_error_code = NULL, lease_token = NULL, lease_until = NULL, \
           next_attempt_at = now(), updated_at = now() \
         WHERE job_id = $1 AND lease_token = $2 AND state IN ('pending', 'unresolved')",
    )
    .bind(&job.job_id)
    .bind(&job.lease_token)
    .bind(schedule_id)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(changed == 1)
}

pub async fn mark_unresolved(
    pool: &PgPool,
    job: &FoundingScheduleJob,
    error: &ProviderError,
) -> Result<bool, sqlx::Error> {
    let code = provider_error_code(error);
    let changed = sqlx::query(
        "UPDATE billing_founding_schedule_jobs SET state = 'unresolved', last_error_code = $3, \
           lease_token = NULL, lease_until = NULL, \
           next_attempt_at = now() + make_interval(secs => LEAST(3600, power(2, LEAST(attempt_count, 10))::integer)), \
           updated_at = now() \
         WHERE job_id = $1 AND lease_token = $2 AND state IN ('pending', 'unresolved')",
    )
    .bind(&job.job_id)
    .bind(&job.lease_token)
    .bind(code)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(changed == 1)
}

pub async fn mark_not_required(
    pool: &PgPool,
    job: &FoundingScheduleJob,
) -> Result<bool, sqlx::Error> {
    let changed = sqlx::query(
        "UPDATE billing_founding_schedule_jobs SET state = 'not_required', \
           last_error_code = 'subscription_terminates_before_transition', lease_token = NULL, \
           lease_until = NULL, updated_at = now(), next_attempt_at = now() \
         WHERE job_id = $1 AND lease_token = $2 AND state IN ('pending', 'unresolved')",
    )
    .bind(&job.job_id)
    .bind(&job.lease_token)
    .execute(pool)
    .await?
    .rows_affected();
    Ok(changed == 1)
}

fn provider_error_code(error: &ProviderError) -> &'static str {
    match error.code.as_deref() {
        Some("local_binding_missing") => return "local_binding_missing",
        Some("local_evidence_invalid") => return "local_evidence_invalid",
        Some("malformed_response") => return "provider_malformed",
        _ => {}
    }
    match error.kind {
        ProviderErrorKind::Authentication => "provider_authentication",
        ProviderErrorKind::ResourceMissing => "provider_missing",
        ProviderErrorKind::Retryable => "provider_retryable",
        ProviderErrorKind::Unknown => "provider_unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_failures_are_reduced_to_fixed_redacted_codes() {
        let error = ProviderError {
            status: Some(401),
            code: Some("secret provider message".into()),
            kind: ProviderErrorKind::Authentication,
        };
        assert_eq!(provider_error_code(&error), "provider_authentication");

        let malformed = ProviderError {
            status: Some(500),
            code: Some("malformed_response".into()),
            kind: ProviderErrorKind::Unknown,
        };
        assert_eq!(provider_error_code(&malformed), "provider_malformed");

        let local = ProviderError {
            status: None,
            code: Some("local_binding_missing".into()),
            kind: ProviderErrorKind::Unknown,
        };
        assert_eq!(provider_error_code(&local), "local_binding_missing");
    }
}
