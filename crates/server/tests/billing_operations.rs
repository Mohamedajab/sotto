//! Database-backed checks for the durable billing operation identity.
//!
//! These tests are opt-in because they create and remove rows in a disposable Postgres database.

use sqlx::PgPool;

use sotto_server::billing_catalogue::BillingOffer;
use sotto_server::billing_operations::{
    begin_operation, load_reconciliation_candidates, record_provider_result, BeginOperation,
    BillingOperationError, BillingOperationRequest, BillingOperationState,
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

fn request(operation_id: &str, request_hash: &str) -> BillingOperationRequest {
    BillingOperationRequest {
        operation_id: operation_id.into(),
        idempotency_key: "billing-operation-test-key".into(),
        request_hash: request_hash.into(),
        actor_user_id: "billing-operation-test-user".into(),
        payer_id: "billing-operation-test-payer".into(),
        beneficiary_id: "billing-operation-test-beneficiary".into(),
        offer: BillingOffer::StandardMonthly,
        quote_version: 1,
        quote_expires_at_epoch: 4_000_000_000,
        provider_idempotency_key: format!("stripe-billing-operation-test-{operation_id}"),
    }
}

async fn cleanup(pool: &PgPool) {
    sqlx::query(
        "DELETE FROM billing_operations WHERE actor_user_id = 'billing-operation-test-user'",
    )
    .execute(pool)
    .await
    .expect("clean billing operation fixtures");
}

#[tokio::test]
async fn operation_identity_is_idempotent_and_conflicts_on_changed_request() {
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    cleanup(&pool).await;

    let first = request("billing-operation-test-first", "hash-a");
    let mut tx = pool.begin().await.expect("begin operation transaction");
    assert!(matches!(
        begin_operation(&mut tx, &first).await.expect("create operation"),
        BeginOperation::Created(operation) if operation.state == BillingOperationState::Pending
    ));
    tx.commit().await.expect("commit operation identity");

    let mut replay_tx = pool.begin().await.expect("begin replay transaction");
    assert!(matches!(
        begin_operation(&mut replay_tx, &first)
            .await
            .expect("replay operation"),
        BeginOperation::AlreadyExists(operation) if operation.operation_id == first.operation_id
    ));
    replay_tx.commit().await.expect("commit replay");

    let conflicting = request("billing-operation-test-other", "hash-b");
    let mut conflict_tx = pool.begin().await.expect("begin conflict transaction");
    assert!(matches!(
        begin_operation(&mut conflict_tx, &conflicting).await,
        Err(BillingOperationError::IdempotencyConflict)
    ));
    conflict_tx.rollback().await.expect("rollback conflict");

    cleanup(&pool).await;
}

#[tokio::test]
async fn unknown_provider_result_remains_a_reconciliation_candidate() {
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    cleanup(&pool).await;

    let operation = request("billing-operation-test-unknown", "hash-unknown");
    let mut tx = pool.begin().await.expect("begin operation transaction");
    begin_operation(&mut tx, &operation)
        .await
        .expect("create operation");
    tx.commit().await.expect("commit operation identity");

    let mut result_tx = pool
        .begin()
        .await
        .expect("begin provider result transaction");
    let stored = record_provider_result(
        &mut result_tx,
        &operation.operation_id,
        BillingOperationState::Unknown,
        None,
        Some("provider_timeout"),
    )
    .await
    .expect("record unknown provider result");
    assert_eq!(stored.state, BillingOperationState::Unknown);
    result_tx.commit().await.expect("commit provider result");

    let candidates = load_reconciliation_candidates(&pool, 10)
        .await
        .expect("load reconciliation candidates");
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].operation_id, operation.operation_id);

    cleanup(&pool).await;
}
