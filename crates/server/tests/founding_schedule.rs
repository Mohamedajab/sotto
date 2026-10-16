//! Database-backed coverage for durable founding-price transition work.

use sqlx::Row;

use sotto_server::{
    db,
    founding_allocator::{FoundingDate, FoundingOffer, FoundingPayerKind},
    founding_schedule,
};

mod support {
    pub mod migrations;
}

use support::migrations::{migrator_before, DisposableDatabase};

#[tokio::test]
async fn legacy_awards_are_unresolved_and_transfer_retargets_the_durable_job() {
    let Some(database) = DisposableDatabase::create().await else {
        eprintln!("skipping founding schedule test: set SOTTO_RUN_DB_TESTS=1");
        return;
    };
    let pool = &database.pool;
    migrator_before(55)
        .run(pool)
        .await
        .expect("migrate to the founding schedule boundary");

    let user = "founding-schedule-beneficiary";
    sqlx::query(
        "INSERT INTO users (id, oauth_provider, oauth_subject) \
         VALUES ($1, 'founding-schedule-test', $1)",
    )
    .bind(user)
    .execute(pool)
    .await
    .expect("insert beneficiary");
    sqlx::query(
        "INSERT INTO billing_operations \
         (operation_id, idempotency_key, request_hash, actor_user_id, payer_id, beneficiary_id, \
          offer, quote_version, quote_expires_at_epoch, provider_idempotency_key) \
         VALUES ('founding-schedule-operation', 'founding-schedule-operation', 'hash', \
          $1, $1, $1, 'founding_monthly', 1, 1800000000, 'founding-schedule-operation')",
    )
    .bind(user)
    .execute(pool)
    .await
    .expect("insert source operation");
    sqlx::query(
        "INSERT INTO billing_personal_accounts \
         (user_id, operation_id, offer, stripe_subscription_id, state, pending_expires_at_epoch) \
         VALUES ($1, 'founding-schedule-operation', 'founding_monthly', \
          'sub_founding_schedule_old', 'active', 1900000000)",
    )
    .bind(user)
    .execute(pool)
    .await
    .expect("insert active personal subscription");
    sqlx::query(
        "INSERT INTO billing_founding_awards \
         (award_id, beneficiary_id, payer_kind, payer_id, offer, cohort_ordinal, \
          original_start_date, original_end_date) \
         VALUES ('founding-schedule-award', $1, 'personal', $1, 'founding_monthly', \
          1, '2026-01-01', '2027-01-01')",
    )
    .bind(user)
    .execute(pool)
    .await
    .expect("insert legacy award without a verified time anchor");

    db::migrate(pool)
        .await
        .expect("apply founding schedule migration");
    assert!(sqlx::query(
        "UPDATE billing_founding_awards SET original_start_epoch = NULL, original_end_epoch = 1 \
         WHERE award_id = 'founding-schedule-award'",
    )
    .execute(pool)
    .await
    .is_err());
    assert!(sqlx::query(
        "UPDATE billing_founding_awards SET original_start_epoch = 1, original_end_epoch = NULL \
         WHERE award_id = 'founding-schedule-award'",
    )
    .execute(pool)
    .await
    .is_err());
    let migrated = sqlx::query(
        "SELECT job.state, job.last_error_code, job.provider_subscription_id, \
                award.original_end_epoch \
         FROM billing_founding_schedule_jobs AS job \
         JOIN billing_founding_awards AS award ON award.award_id = job.award_id \
         WHERE job.award_id = 'founding-schedule-award'",
    )
    .fetch_one(pool)
    .await
    .expect("legacy subscription is represented by an unresolved job");
    assert_eq!(
        migrated.try_get::<String, _>("state").unwrap(),
        "unresolved"
    );
    assert_eq!(
        migrated.try_get::<String, _>("last_error_code").unwrap(),
        "local_evidence_invalid"
    );
    assert_eq!(
        migrated
            .try_get::<String, _>("provider_subscription_id")
            .unwrap(),
        "sub_founding_schedule_old"
    );
    assert_eq!(
        migrated
            .try_get::<Option<i64>, _>("original_end_epoch")
            .unwrap(),
        None,
        "migration must not guess a legacy award's verified Stripe boundary"
    );

    let award = sotto_server::founding_allocator::FoundingAward {
        award_id: "founding-schedule-award".into(),
        beneficiary_id: user.into(),
        payer_kind: FoundingPayerKind::Personal,
        payer_id: user.into(),
        offer: FoundingOffer::Monthly,
        cohort_ordinal: 1,
        original_start: FoundingDate::parse("2026-01-01").unwrap(),
        original_end: FoundingDate::parse("2027-01-01").unwrap(),
        original_start_epoch: None,
        original_end_epoch: None,
    };
    let mut tx = pool.begin().await.expect("begin queue retarget");
    founding_schedule::enqueue(&mut tx, &award, "sub_founding_schedule_new")
        .await
        .expect("retarget the job to the verified destination subscription");
    tx.commit().await.expect("commit retarget");

    let stale = founding_schedule::claim_due(pool)
        .await
        .expect("claim first retargeted work")
        .expect("first retargeted work is due");
    assert_eq!(stale.subscription_id, "sub_founding_schedule_new");
    assert_eq!(
        stale.idempotency_key,
        "founding-transition:founding-schedule-award:2"
    );
    let mut tx = pool.begin().await.expect("begin second retarget");
    founding_schedule::enqueue(&mut tx, &award, "sub_founding_schedule_final")
        .await
        .expect("a newer destination supersedes the leased work");
    tx.commit().await.expect("commit second retarget");
    assert!(
        !founding_schedule::mark_scheduled(pool, &stale, "sched_stale")
            .await
            .expect("stale completion is checked conditionally")
    );

    let current = founding_schedule::claim_due(pool)
        .await
        .expect("claim current work")
        .expect("new version remains due");
    assert_eq!(current.subscription_id, "sub_founding_schedule_final");
    assert_eq!(
        current.idempotency_key,
        "founding-transition:founding-schedule-award:3"
    );
    assert!(
        founding_schedule::mark_scheduled(pool, &current, "sched_current")
            .await
            .expect("record current verified schedule")
    );
    let mut tx = pool.begin().await.expect("begin idempotent enqueue");
    founding_schedule::enqueue(&mut tx, &award, "sub_founding_schedule_final")
        .await
        .expect("same target is a no-op");
    tx.commit().await.expect("commit idempotent enqueue");
    let state: String = sqlx::query_scalar(
        "SELECT state FROM billing_founding_schedule_jobs WHERE award_id = 'founding-schedule-award'",
    )
    .fetch_one(pool)
    .await
    .expect("load final schedule job state");
    assert_eq!(state, "scheduled");

    database.cleanup().await;
}
