//! Opt-in Postgres coverage for durable personal billing state.

use std::str::FromStr;

use sqlx::{postgres::PgConnectOptions, PgPool};

use sotto_server::billing_catalogue::BillingOffer;
use sotto_server::billing_operations::{begin_personal_operation, BeginOperation};
use sotto_server::db;
use sotto_server::personal_billing::{
    begin_account, load_account, record_paid_settlement, PersonalBillingError, PersonalBillingState,
};

async fn pool_or_skip() -> Option<PgPool> {
    if std::env::var("SOTTO_RUN_DB_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping personal billing tests: set SOTTO_RUN_DB_TESTS=1");
        return None;
    }
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL for opted-in database tests");
    let options = PgConnectOptions::from_str(&url).expect("parse DATABASE_URL");
    assert!(
        matches!(options.get_host(), "localhost" | "127.0.0.1" | "::1"),
        "refusing personal billing tests against non-local host: {}",
        options.get_host()
    );
    let pool = db::connect(&url).await.expect("connect to test database");
    db::migrate(&pool).await.expect("apply migrations");
    Some(pool)
}

async fn cleanup(pool: &PgPool, user_id: &str) {
    sqlx::query("DELETE FROM billing_personal_accounts WHERE user_id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("clean personal account");
    sqlx::query("DELETE FROM billing_operations WHERE actor_user_id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("clean personal operation");
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(user_id)
        .execute(pool)
        .await
        .expect("clean personal user");
}

#[tokio::test]
async fn personal_settlement_is_idempotent_and_keeps_paid_term() {
    let Some(pool) = pool_or_skip().await else {
        return;
    };
    const USER_ID: &str = "personal-billing-test-user";
    cleanup(&pool, USER_ID).await;
    sqlx::query("INSERT INTO users (id, oauth_provider, oauth_subject) VALUES ($1, 'test', $1)")
        .bind(USER_ID)
        .execute(&pool)
        .await
        .expect("seed personal user");

    let mut tx = pool.begin().await.expect("begin checkout operation");
    let operation = match begin_personal_operation(
        &mut tx,
        USER_ID,
        "personal-billing-test-operation",
        BillingOffer::FoundingMonthly,
        1,
        1_900_000_000,
        "https://app.sotto.test/billing",
        "https://app.sotto.test/billing",
    )
    .await
    .expect("begin operation")
    {
        BeginOperation::Created(operation) => operation,
        BeginOperation::AlreadyExists(_) => panic!("operation fixture unexpectedly existed"),
    };
    begin_account(
        &mut tx,
        USER_ID,
        &operation.operation_id,
        BillingOffer::FoundingMonthly,
    )
    .await
    .expect("begin account");
    tx.commit().await.expect("commit pending account");

    let mut settlement_tx = pool.begin().await.expect("begin settlement");
    record_paid_settlement(
        &mut settlement_tx,
        &operation.operation_id,
        "cus_personal_test",
        "sub_personal_test",
        "pi_personal_test",
        1_800_086_400,
        "2027-01-01",
    )
    .await
    .expect("record settlement");
    record_paid_settlement(
        &mut settlement_tx,
        &operation.operation_id,
        "cus_personal_test",
        "sub_personal_test",
        "pi_personal_test",
        1_800_086_400,
        "2027-01-01",
    )
    .await
    .expect("replay settlement");
    assert!(matches!(
        record_paid_settlement(
            &mut settlement_tx,
            &operation.operation_id,
            "cus_other",
            "sub_other",
            "pi_other",
            1_800_086_400,
            "2027-01-01",
        )
        .await,
        Err(PersonalBillingError::SettlementConflict)
    ));
    settlement_tx.commit().await.expect("commit settlement");

    let mut load_tx = pool.begin().await.expect("begin account load");
    let account = load_account(&mut load_tx, USER_ID)
        .await
        .expect("load account")
        .expect("account exists");
    assert_eq!(account.state, PersonalBillingState::Active);
    assert_eq!(account.paid_through_date.as_deref(), Some("2027-01-01"));
    assert_eq!(
        account.payment_reference.as_deref(),
        Some("pi_personal_test")
    );
    load_tx.rollback().await.expect("rollback account load");
    cleanup(&pool, USER_ID).await;
}
