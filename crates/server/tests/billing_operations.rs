//! Database-backed checks for the durable billing operation identity.
//!
//! These tests are opt-in because they create and remove rows in a disposable Postgres database.

use async_trait::async_trait;
use sqlx::PgPool;

use sotto_server::billing_catalogue::BillingOffer;
use sotto_server::billing_operations::{
    begin_personal_operation, load_reconciliation_candidates, reconcile_operation,
    record_provider_result, BeginOperation, BillingOperation, BillingOperationError,
    BillingOperationProvider, BillingOperationState, ProviderResolution,
};
use sotto_server::db;

async fn pool_or_skip() -> Option<PgPool> {
    if std::env::var("SOTTO_RUN_DB_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping billing operation tests: set SOTTO_RUN_DB_TESTS=1");
        return None;
    }
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL for opted-in database tests");
    let pool = db::connect(&url).await.expect("connect to test database");
    db::migrate(&pool).await.expect("apply migrations");
    Some(pool)
}

async fn cleanup(pool: &PgPool, user_id: &str) {
    sqlx::query("DELETE FROM billing_operations WHERE actor_user_id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("clean billing operation fixtures");
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("clean billing operation user fixture");
}

async fn seed_user(pool: &PgPool, user_id: &str) {
    sqlx::query(
        "INSERT INTO users (id, oauth_provider, oauth_subject) VALUES ($1, $2, $1) \
         ON CONFLICT (id) DO NOTHING",
    )
    .bind(user_id)
    .bind(format!("billing-operation-test-{user_id}"))
    .execute(pool)
    .await
    .expect("seed billing operation user fixture");
}

#[tokio::test]
async fn operation_identity_is_idempotent_and_conflicts_on_changed_request() {
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    const USER_ID: &str = "billing-operation-test-idempotency";
    cleanup(&pool, USER_ID).await;
    seed_user(&pool, USER_ID).await;

    let mut tx = pool.begin().await.expect("begin operation transaction");
    let first_operation_id = match begin_personal_operation(
        &mut tx,
        USER_ID,
        "billing-operation-test-key",
        BillingOffer::StandardMonthly,
        1,
        4_000_000_000,
        "https://app.sotto.test/billing",
        "https://app.sotto.test/billing",
    )
    .await
    .expect("create operation")
    {
        BeginOperation::Created(operation) => {
            assert_eq!(operation.state, BillingOperationState::Pending);
            operation.operation_id
        }
        BeginOperation::AlreadyExists(_) => panic!("fixture operation unexpectedly existed"),
    };
    tx.commit().await.expect("commit operation identity");

    let mut replay_tx = pool.begin().await.expect("begin replay transaction");
    assert!(matches!(
        begin_personal_operation(
            &mut replay_tx,
            USER_ID,
            "billing-operation-test-key",
            BillingOffer::StandardMonthly,
            1,
            4_000_000_000,
            "https://app.sotto.test/billing",
            "https://app.sotto.test/billing",
        )
            .await
            .expect("replay operation"),
        BeginOperation::AlreadyExists(operation) if operation.operation_id == first_operation_id
    ));
    replay_tx.commit().await.expect("commit replay");

    let mut conflict_tx = pool.begin().await.expect("begin conflict transaction");
    assert!(matches!(
        begin_personal_operation(
            &mut conflict_tx,
            USER_ID,
            "billing-operation-test-key",
            BillingOffer::StandardAnnual,
            1,
            4_000_000_000,
            "https://app.sotto.test/billing",
            "https://app.sotto.test/billing",
        )
        .await,
        Err(BillingOperationError::IdempotencyConflict)
    ));
    conflict_tx.rollback().await.expect("rollback conflict");

    cleanup(&pool, USER_ID).await;
}

struct SuccessfulProvider;

#[async_trait]
impl BillingOperationProvider for SuccessfulProvider {
    async fn resolve(
        &self,
        _operation: &BillingOperation,
    ) -> Result<ProviderResolution, sotto_server::billing_operations::BillingRecoveryError> {
        Ok(ProviderResolution::Succeeded {
            provider_operation_id: "pi_recovered".into(),
            result_code: "paid".into(),
        })
    }
}

#[tokio::test]
async fn unknown_provider_result_is_reconciled_after_restart() {
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    const USER_ID: &str = "billing-operation-test-recovery";
    cleanup(&pool, USER_ID).await;
    seed_user(&pool, USER_ID).await;

    let mut tx = pool.begin().await.expect("begin operation transaction");
    let operation_id = match begin_personal_operation(
        &mut tx,
        USER_ID,
        "billing-operation-test-key",
        BillingOffer::StandardMonthly,
        1,
        4_000_000_000,
        "https://app.sotto.test/billing",
        "https://app.sotto.test/billing",
    )
    .await
    .expect("create operation")
    {
        BeginOperation::Created(operation) => operation.operation_id,
        BeginOperation::AlreadyExists(_) => panic!("fixture operation unexpectedly existed"),
    };
    tx.commit().await.expect("commit operation identity");

    let mut result_tx = pool
        .begin()
        .await
        .expect("begin provider result transaction");
    let stored = record_provider_result(
        &mut result_tx,
        &operation_id,
        BillingOperationState::Unknown,
        None,
        Some("provider_timeout"),
    )
    .await
    .expect("record unknown provider result");
    assert_eq!(stored.state, BillingOperationState::Unknown);
    result_tx.commit().await.expect("commit provider result");

    let mut replay_tx = pool
        .begin()
        .await
        .expect("begin provider replay transaction");
    let replayed = record_provider_result(
        &mut replay_tx,
        &operation_id,
        BillingOperationState::Unknown,
        None,
        Some("provider_timeout"),
    )
    .await
    .expect("replay identical provider result");
    assert_eq!(replayed.state, BillingOperationState::Unknown);
    replay_tx.commit().await.expect("commit provider replay");

    let mut conflicting_tx = pool
        .begin()
        .await
        .expect("begin conflicting provider result transaction");
    assert!(matches!(
        record_provider_result(
            &mut conflicting_tx,
            &operation_id,
            BillingOperationState::Succeeded,
            Some("pi_conflicting"),
            Some("paid"),
        )
        .await,
        Err(BillingOperationError::ResultConflict)
    ));
    conflicting_tx
        .rollback()
        .await
        .expect("rollback conflicting provider result");

    let candidates = load_reconciliation_candidates(&pool, 10)
        .await
        .expect("load reconciliation candidates");
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].operation_id, operation_id);

    let resolved = reconcile_operation(&pool, &SuccessfulProvider, &operation_id)
        .await
        .expect("reconcile provider result after restart");
    assert_eq!(resolved.state, BillingOperationState::Succeeded);
    assert_eq!(
        resolved.provider_operation_id.as_deref(),
        Some("pi_recovered")
    );
    assert!(load_reconciliation_candidates(&pool, 10)
        .await
        .expect("load resolved candidates")
        .is_empty());

    cleanup(&pool, USER_ID).await;
}
