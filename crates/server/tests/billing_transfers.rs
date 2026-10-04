//! Opt-in Postgres coverage for beneficiary-scoped payer transfer intents.

use std::str::FromStr;

use sqlx::{postgres::PgConnectOptions, PgPool};
use uuid::Uuid;

use sotto_server::billing_catalogue::BillingOffer;
use sotto_server::billing_transfers::{
    begin_source_adjustment, begin_transfer, complete_source_adjustment, mark_failed,
    record_destination_paid, record_destination_prepared, BeginTransfer, TransferError,
    TransferPayer, TransferRequest, TransferState,
};
use sotto_server::db;

async fn pool_or_skip() -> Option<PgPool> {
    if std::env::var("SOTTO_RUN_DB_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping billing transfer tests: SOTTO_RUN_DB_TESTS=1 not set");
        return None;
    }
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL for opted-in database tests");
    let options = PgConnectOptions::from_str(&url).expect("parse DATABASE_URL");
    assert!(
        matches!(options.get_host(), "localhost" | "127.0.0.1" | "::1"),
        "refusing billing transfer tests against non-local host: {}",
        options.get_host()
    );
    let pool = db::connect(&url).await.expect("connect to test database");
    db::migrate(&pool).await.expect("apply migrations");
    Some(pool)
}

async fn cleanup(pool: &PgPool, fixture: &str) {
    let pattern = format!("transfer-test-{fixture}-%");
    sqlx::query(
        "DELETE FROM billing_transfer_intents
         WHERE actor_user_id LIKE $1 OR beneficiary_id LIKE $1
            OR source_organization_id LIKE $1 OR destination_organization_id LIKE $1",
    )
    .bind(&pattern)
    .execute(pool)
    .await
    .expect("clean transfer intents");
    sqlx::query("DELETE FROM billing_founding_awards WHERE beneficiary_id LIKE $1")
        .bind(&pattern)
        .execute(pool)
        .await
        .expect("clean founding awards");
    sqlx::query("DELETE FROM billing_sponsored_seats WHERE seat_id LIKE $1")
        .bind(&pattern)
        .execute(pool)
        .await
        .expect("clean sponsored seats");
    sqlx::query("DELETE FROM billing_sponsored_operations WHERE operation_id LIKE $1")
        .bind(&pattern)
        .execute(pool)
        .await
        .expect("clean sponsored operations");
    sqlx::query("DELETE FROM billing_personal_accounts WHERE user_id LIKE $1")
        .bind(&pattern)
        .execute(pool)
        .await
        .expect("clean personal accounts");
    sqlx::query("DELETE FROM billing_operations WHERE operation_id LIKE $1")
        .bind(&pattern)
        .execute(pool)
        .await
        .expect("clean billing operations");
    sqlx::query("DELETE FROM organization_memberships WHERE org_id LIKE $1")
        .bind(&pattern)
        .execute(pool)
        .await
        .expect("clean memberships");
    sqlx::query("DELETE FROM organizations WHERE id LIKE $1")
        .bind(&pattern)
        .execute(pool)
        .await
        .expect("clean organisations");
    sqlx::query("DELETE FROM users WHERE id LIKE $1")
        .bind(&pattern)
        .execute(pool)
        .await
        .expect("clean users");
}

fn personal_to_sponsor_request(
    actor: &str,
    beneficiary: &str,
    org: &str,
    key: &str,
) -> TransferRequest {
    TransferRequest {
        actor_user_id: actor.into(),
        beneficiary_id: beneficiary.into(),
        source_kind: TransferPayer::Personal,
        source_organization_id: None,
        destination_kind: TransferPayer::Sponsor,
        destination_organization_id: Some(org.into()),
        offer: BillingOffer::FoundingMonthly,
        quote_version: 1,
        quote_expires_at_epoch: 1_800_001_000,
        effective_from: 1_800_000_000,
        effective_until: None,
        idempotency_key: key.into(),
    }
}

#[tokio::test]
async fn transfer_is_idempotent_and_moves_founding_payer_after_paid_destination() {
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    let fixture = Uuid::new_v4().to_string();
    let user = format!("transfer-test-{fixture}-user");
    let org = format!("transfer-test-{fixture}-org");
    let operation = format!("transfer-test-{fixture}-personal-operation");
    let award = format!("transfer-test-{fixture}-award");
    cleanup(&pool, &fixture).await;

    sqlx::query(
        "INSERT INTO users (id, oauth_provider, oauth_subject) VALUES ($1, 'transfer-test', $1)",
    )
    .bind(&user)
    .execute(&pool)
    .await
    .expect("insert beneficiary");
    sqlx::query("INSERT INTO organizations (id, enc_name, created_by) VALUES ($1, decode('6f7267', 'hex'), $2)")
        .bind(&org)
        .bind(&user)
        .execute(&pool)
        .await
        .expect("insert destination organisation");
    sqlx::query(
        "INSERT INTO organization_memberships (org_id, user_id, role) VALUES ($1, $2, 'owner')",
    )
    .bind(&org)
    .bind(&user)
    .execute(&pool)
    .await
    .expect("insert destination owner");
    sqlx::query(
        "INSERT INTO billing_operations
         (operation_id, idempotency_key, request_hash, actor_user_id, payer_id, beneficiary_id,
          offer, quote_version, quote_expires_at_epoch, provider_idempotency_key)
         VALUES ($1, $1, $1, $2, $2, $2, 'founding_monthly', 1, 1800001000, $1)",
    )
    .bind(&operation)
    .bind(&user)
    .execute(&pool)
    .await
    .expect("insert source operation");
    sqlx::query(
        "INSERT INTO billing_personal_accounts
         (user_id, operation_id, offer, stripe_subscription_id, state, pending_expires_at_epoch)
         VALUES ($1, $2, 'founding_monthly', $3, 'active', 1900000000)",
    )
    .bind(&user)
    .bind(&operation)
    .bind(format!("sub_transfer_source_{fixture}"))
    .execute(&pool)
    .await
    .expect("insert active personal source");
    let cohort_ordinal: i32 = sqlx::query_scalar(
        "SELECT candidate FROM generate_series(1, 100) AS series(candidate)
         WHERE NOT EXISTS (SELECT 1 FROM billing_founding_awards WHERE cohort_ordinal = candidate)
         LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("find available founding place");
    sqlx::query(
        "INSERT INTO billing_founding_awards
         (award_id, beneficiary_id, payer_id, offer, cohort_ordinal, original_start_date, original_end_date)
         VALUES ($1, $2, $2, 'founding_monthly', $3, '2026-01-01', '2026-02-01')",
    )
    .bind(&award)
    .bind(&user)
    .bind(cohort_ordinal)
    .execute(&pool)
    .await
    .expect("insert founding award");

    let request = personal_to_sponsor_request(&user, &user, &org, "transfer-key");
    let mut tx = pool.begin().await.expect("begin transfer");
    let created = begin_transfer(&mut tx, &request, 1_800_000_000)
        .await
        .expect("create transfer");
    let transfer = match created {
        BeginTransfer::Created(transfer) => transfer,
        BeginTransfer::AlreadyExists(_) => panic!("first transfer unexpectedly existed"),
    };
    tx.commit().await.expect("commit transfer");

    let mut tx = pool.begin().await.expect("begin replay");
    let replay = begin_transfer(&mut tx, &request, 1_800_000_001)
        .await
        .expect("replay transfer");
    tx.commit().await.expect("commit replay");
    let replayed = match replay {
        BeginTransfer::AlreadyExists(transfer) => transfer,
        BeginTransfer::Created(_) => panic!("replay created another transfer"),
    };
    assert_eq!(replayed.transfer_id, transfer.transfer_id);

    let mut conflicting = request.clone();
    conflicting.effective_from += 1;
    let mut tx = pool.begin().await.expect("begin conflict");
    assert!(matches!(
        begin_transfer(&mut tx, &conflicting, 1_800_000_000).await,
        Err(TransferError::ResultConflict)
    ));
    tx.rollback().await.expect("rollback conflict");

    let mut tx = pool.begin().await.expect("begin prepare");
    record_destination_prepared(
        &mut tx,
        &transfer.transfer_id,
        "destination-operation",
        Some("sub_destination"),
    )
    .await
    .expect("record destination preparation");
    record_destination_paid(&mut tx, &transfer.transfer_id, "pi_destination")
        .await
        .expect("record destination payment");
    begin_source_adjustment(&mut tx, &transfer.transfer_id, "source-operation")
        .await
        .expect("begin source adjustment");
    complete_source_adjustment(&mut tx, &transfer.transfer_id, "credit_source")
        .await
        .expect("complete source adjustment");
    tx.commit().await.expect("commit transfer completion");

    let state: String =
        sqlx::query_scalar("SELECT state FROM billing_transfer_intents WHERE transfer_id = $1")
            .bind(&transfer.transfer_id)
            .fetch_one(&pool)
            .await
            .expect("load transfer state");
    assert_eq!(state, TransferState::Completed.as_str());
    let payer: String =
        sqlx::query_scalar("SELECT payer_id FROM billing_founding_awards WHERE award_id = $1")
            .bind(&award)
            .fetch_one(&pool)
            .await
            .expect("load founding payer");
    assert_eq!(payer, org);

    let mut tx = pool.begin().await.expect("begin callback replay");
    record_destination_prepared(
        &mut tx,
        &transfer.transfer_id,
        "destination-operation",
        Some("sub_destination"),
    )
    .await
    .expect("matching preparation retry remains idempotent");
    record_destination_paid(&mut tx, &transfer.transfer_id, "pi_destination")
        .await
        .expect("matching payment retry remains idempotent");
    assert!(matches!(
        record_destination_paid(&mut tx, &transfer.transfer_id, "pi_other").await,
        Err(TransferError::ResultConflict)
    ));
    assert!(matches!(
        mark_failed(&mut tx, &transfer.transfer_id, "late_failure").await,
        Err(TransferError::InvalidTransition)
    ));
    tx.rollback().await.expect("rollback callback replay");

    cleanup(&pool, &fixture).await;
}

#[tokio::test]
async fn concurrent_live_transfer_is_rejected_and_personal_actor_is_required() {
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    let fixture = Uuid::new_v4().to_string();
    let user = format!("transfer-test-{fixture}-user");
    let other = format!("transfer-test-{fixture}-other");
    let org = format!("transfer-test-{fixture}-org");
    let operation = format!("transfer-test-{fixture}-operation");
    cleanup(&pool, &fixture).await;
    sqlx::query(
        "INSERT INTO users (id, oauth_provider, oauth_subject) VALUES
         ($1, 'transfer-test', $1), ($2, 'transfer-test', $2)",
    )
    .bind(&user)
    .bind(&other)
    .execute(&pool)
    .await
    .expect("insert users");
    sqlx::query("INSERT INTO organizations (id, enc_name, created_by) VALUES ($1, decode('6f7267', 'hex'), $2)")
        .bind(&org)
        .bind(&other)
        .execute(&pool)
        .await
        .expect("insert organisation");
    sqlx::query(
        "INSERT INTO organization_memberships (org_id, user_id, role) VALUES
         ($1, $2, 'owner'), ($1, $3, 'member')",
    )
    .bind(&org)
    .bind(&user)
    .bind(&other)
    .execute(&pool)
    .await
    .expect("insert owner");
    sqlx::query(
        "INSERT INTO billing_operations
         (operation_id, idempotency_key, request_hash, actor_user_id, payer_id, beneficiary_id,
          offer, quote_version, quote_expires_at_epoch, provider_idempotency_key)
         VALUES ($1, $1, $1, $2, $2, $2, 'standard_monthly', 1, 1800001000, $1)",
    )
    .bind(&operation)
    .bind(&user)
    .execute(&pool)
    .await
    .expect("insert source operation");
    sqlx::query(
        "INSERT INTO billing_personal_accounts
         (user_id, operation_id, offer, stripe_subscription_id, state, pending_expires_at_epoch)
         VALUES ($1, $2, 'standard_monthly', $3, 'active', 1900000000)",
    )
    .bind(&user)
    .bind(&operation)
    .bind(format!("sub_transfer_source_{fixture}"))
    .execute(&pool)
    .await
    .expect("insert active account");

    let request = personal_to_sponsor_request(&user, &user, &org, "live-transfer");
    let mut tx = pool.begin().await.expect("begin first transfer");
    begin_transfer(&mut tx, &request, 1_800_000_000)
        .await
        .expect("create first transfer");
    tx.commit().await.expect("commit first transfer");

    let mut second = request.clone();
    second.idempotency_key = "live-transfer-second".into();
    let mut tx = pool.begin().await.expect("begin second transfer");
    assert!(matches!(
        begin_transfer(&mut tx, &second, 1_800_000_000).await,
        Err(TransferError::AlreadyInProgress)
    ));
    tx.rollback().await.expect("rollback second transfer");

    let mut unauthorised = request;
    unauthorised.actor_user_id = other;
    unauthorised.idempotency_key = "unauthorised-transfer".into();
    let mut tx = pool.begin().await.expect("begin unauthorised transfer");
    assert!(matches!(
        begin_transfer(&mut tx, &unauthorised, 1_800_000_000).await,
        Err(TransferError::Unauthorised)
    ));
    tx.rollback().await.expect("rollback unauthorised transfer");

    cleanup(&pool, &fixture).await;
}
