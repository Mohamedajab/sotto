//! Database coverage for named sponsor seat intents.
//!
//! These tests are opt-in like the other PostgreSQL integration targets. They use unique fixture
//! identities and remove the operation rows explicitly because their foreign keys preserve history.

use sqlx::postgres::PgConnectOptions;
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

use sotto_server::billing_catalogue::BillingOffer;
use sotto_server::db;
use sotto_server::sponsored_billing::{
    begin_operation, complete_paid_checkout, record_checkout, SponsoredBillingError,
    SponsoredSeatAction, SponsoredSeatRequest,
};

async fn pool() -> Option<PgPool> {
    if std::env::var("SOTTO_RUN_DB_TESTS").as_deref() != Ok("1") {
        eprintln!("skipping sponsored billing tests: SOTTO_RUN_DB_TESTS=1 not set");
        return None;
    }
    let url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping sponsored billing tests: DATABASE_URL not set");
            return None;
        }
    };
    let options = PgConnectOptions::from_str(&url).expect("parse DATABASE_URL");
    assert!(
        matches!(options.get_host(), "localhost" | "127.0.0.1" | "::1"),
        "refusing sponsored billing tests against non-local host: {}",
        options.get_host()
    );
    let pool = db::connect(&url).await.expect("connect");
    db::migrate(&pool).await.expect("migrate");
    Some(pool)
}

#[tokio::test]
async fn named_seat_operation_is_idempotent_and_activates_after_paid_result() {
    let Some(pool) = pool().await else {
        return;
    };
    let suffix = Uuid::new_v4().to_string();
    let org_id = format!("sponsored-org-{suffix}");
    let owner = format!("sponsored-owner-{suffix}");
    let beneficiary = format!("sponsored-beneficiary-{suffix}");
    sqlx::query(
        "INSERT INTO users (id, oauth_provider, oauth_subject) VALUES \
         ($1, 'sponsored-test', $1), ($2, 'sponsored-test', $2)",
    )
    .bind(&owner)
    .bind(&beneficiary)
    .execute(&pool)
    .await
    .expect("insert users");
    sqlx::query(
        "INSERT INTO organizations (id, enc_name, created_by) \
         VALUES ($1, decode('6f7267', 'hex'), $2)",
    )
    .bind(&org_id)
    .bind(&owner)
    .execute(&pool)
    .await
    .expect("insert organisation");
    sqlx::query(
        "INSERT INTO organization_memberships (org_id, user_id, role) VALUES ($1, $2, 'owner')",
    )
    .bind(&org_id)
    .bind(&owner)
    .execute(&pool)
    .await
    .expect("insert owner membership");

    let request = SponsoredSeatRequest {
        action: SponsoredSeatAction::Add,
        beneficiary_id: beneficiary.clone(),
        replacement_beneficiary_id: None,
        offer: BillingOffer::StandardMonthly,
        quote_version: 1,
        quote_expires_at_epoch: 4_000_000_000,
        effective_from: 1_700_000_001,
        effective_until: None,
        idempotency_key: "seat-add-1".into(),
    };
    let mut tx = pool.begin().await.expect("begin operation transaction");
    let first = begin_operation(&mut tx, &org_id, &owner, &request, 1_700_000_001)
        .await
        .expect("create operation");
    tx.commit().await.expect("commit operation");

    let mut tx = pool.begin().await.expect("begin replay transaction");
    let replay = begin_operation(&mut tx, &org_id, &owner, &request, 1_700_000_001)
        .await
        .expect("replay operation");
    tx.commit().await.expect("commit replay");
    assert_eq!(first.operation_id, replay.operation_id);

    let mut tx = pool.begin().await.expect("begin late replay transaction");
    let late_replay = begin_operation(&mut tx, &org_id, &owner, &request, 5_000_000_000)
        .await
        .expect("late replay keeps the original operation recoverable");
    tx.commit().await.expect("commit late replay");
    assert_eq!(first.operation_id, late_replay.operation_id);

    let mut tx = pool.begin().await.expect("begin checkout transaction");
    record_checkout(&mut tx, &first.operation_id, "https://stripe.test/checkout")
        .await
        .expect("record checkout");
    tx.commit().await.expect("commit checkout");

    let mut tx = pool.begin().await.expect("begin settlement transaction");
    let settled = complete_paid_checkout(&mut tx, &first.operation_id, "pi_sponsored_test")
        .await
        .expect("settle operation");
    tx.commit().await.expect("commit settlement");
    assert_eq!(settled.state, "active");
    let state: String =
        sqlx::query_scalar("SELECT state FROM billing_sponsored_seats WHERE operation_id = $1")
            .bind(&first.operation_id)
            .fetch_one(&pool)
            .await
            .expect("load seat state");
    assert_eq!(state, "active");

    let mut tx = pool.begin().await.expect("begin conflicting settlement");
    let conflict = complete_paid_checkout(&mut tx, &first.operation_id, "pi_other")
        .await
        .expect_err("a different provider result must conflict");
    assert!(matches!(conflict, SponsoredBillingError::ResultConflict));
    tx.rollback()
        .await
        .expect("rollback conflicting settlement");

    sqlx::query("UPDATE organizations SET lifecycle_state = 'deleting' WHERE id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("mark organisation deleting");
    let mut tx = pool.begin().await.expect("begin deleting settlement");
    let deletion_error = complete_paid_checkout(&mut tx, &first.operation_id, "pi_sponsored_test")
        .await
        .expect_err("a deleting organisation cannot settle another webhook");
    assert!(matches!(
        deletion_error,
        SponsoredBillingError::OrganisationNotActive
    ));
    tx.rollback().await.expect("rollback deleting settlement");
    sqlx::query("UPDATE organizations SET lifecycle_state = 'active' WHERE id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("restore organisation lifecycle");

    sqlx::query("DELETE FROM billing_sponsored_seats WHERE organization_id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete seats");
    sqlx::query("DELETE FROM billing_sponsored_operations WHERE organization_id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete operations");
    sqlx::query("DELETE FROM organization_memberships WHERE org_id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete membership");
    sqlx::query("DELETE FROM organizations WHERE id = $1")
        .bind(&org_id)
        .execute(&pool)
        .await
        .expect("delete organisation");
    sqlx::query("DELETE FROM users WHERE id IN ($1, $2)")
        .bind(&owner)
        .bind(&beneficiary)
        .execute(&pool)
        .await
        .expect("delete users");
}
