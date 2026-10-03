//! Database acceptance for immutable, dated sponsored allocation terms.

use sqlx::PgPool;

use sotto_server::cloud_provider::{
    AllocationState, PayerKind, ProviderContext, ProviderEnvironment, VerifiedAllocation,
};
use sotto_server::cloud_provider_stripe_sponsored_store::{
    load_sponsored_allocation_manifest, record_sponsored_allocation_term,
    SponsoredAllocationTermDisposition, SponsoredAllocationTermStoreError,
    SponsoredManifestLoadError, SponsoredManifestLoadLimits,
};
use sotto_server::db;

#[allow(dead_code)]
mod support {
    pub mod migrations;
}

use support::migrations::DisposableDatabase;

struct Fixture {
    database: DisposableDatabase,
    context: ProviderContext,
    allocation: VerifiedAllocation,
}

impl Fixture {
    async fn create() -> Option<Self> {
        let database = DisposableDatabase::create().await?;
        db::migrate(&database.pool).await.expect("migrate");
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let beneficiary_id = format!("sponsored-term-beneficiary-{suffix}");
        let payer_id = format!("sponsored-term-payer-{suffix}");
        let allocation_id = format!("sponsored-term-allocation-{suffix}");
        let source_id = format!("sponsored-term-source-{suffix}");
        let external_reference = format!("sponsored-term-reference-{suffix}");
        let ownership_reference = format!("sponsored-term-ownership-{suffix}");
        let context = ProviderContext::new(
            "stripe",
            format!("account-{suffix}"),
            ProviderEnvironment::Test,
        )
        .expect("provider context");
        let allocation = VerifiedAllocation::new(
            &allocation_id,
            &payer_id,
            format!("customer-{suffix}"),
            PayerKind::Sponsor,
            &beneficiary_id,
            format!("subscription-{suffix}"),
            format!("item-{suffix}"),
            &external_reference,
            &source_id,
            100,
            Some(200),
            AllocationState::Active,
            &ownership_reference,
        )
        .expect("allocation");

        insert_fixture_rows(&database.pool, &context, &allocation).await;
        Some(Self {
            database,
            context,
            allocation,
        })
    }
}

async fn insert_fixture_rows(
    pool: &PgPool,
    context: &ProviderContext,
    allocation: &VerifiedAllocation,
) {
    sqlx::query(
        "INSERT INTO users (id, oauth_provider, oauth_subject) VALUES ($1, 'sponsored-term', $1)",
    )
    .bind(&allocation.beneficiary_id)
    .execute(pool)
    .await
    .expect("insert beneficiary");
    sqlx::query(
        "INSERT INTO cloud_coverage_sources \
         (source_id, beneficiary_id, provider_namespace, external_allocation_reference, \
          ownership_evidence_reference, registration_operation_id, registration_source_set_generation) \
         VALUES ($1, $2, 'stripe', $3, $4, $5, 1)",
    )
    .bind(&allocation.source_id)
    .bind(&allocation.beneficiary_id)
    .bind(&allocation.external_allocation_reference)
    .bind(&allocation.ownership_evidence_reference)
    .bind(format!("sponsored-term-registration-{}", allocation.beneficiary_id))
    .execute(pool)
    .await
    .expect("insert source");
    sqlx::query(
        "INSERT INTO cloud_provider_payers \
         (payer_id, provider_namespace, provider_account_id, provider_environment, \
          provider_customer_id, payer_kind) VALUES ($1, 'stripe', $2, 'test', $3, 'sponsor')",
    )
    .bind(&allocation.payer_id)
    .bind(&context.account_id)
    .bind(&allocation.provider_customer_id)
    .execute(pool)
    .await
    .expect("insert payer");
    sqlx::query(
        "INSERT INTO cloud_provider_allocations \
         (allocation_id, payer_id, payer_kind, beneficiary_id, provider_namespace, provider_account_id, \
          provider_environment, provider_subscription_id, provider_item_id, external_allocation_reference, \
          coverage_source_id, effective_from, effective_until, state, ownership_evidence_reference) \
         VALUES ($1, $2, 'sponsor', $3, 'stripe', $4, 'test', $5, $6, $7, $8, $9, $10, 'active', $11)",
    )
    .bind(&allocation.allocation_id)
    .bind(&allocation.payer_id)
    .bind(&allocation.beneficiary_id)
    .bind(&context.account_id)
    .bind(&allocation.subscription_id)
    .bind(&allocation.provider_item_id)
    .bind(&allocation.external_allocation_reference)
    .bind(&allocation.source_id)
    .bind(allocation.effective_from)
    .bind(allocation.effective_until)
    .bind(&allocation.ownership_evidence_reference)
    .execute(pool)
    .await
    .expect("insert allocation");
}

#[tokio::test]
async fn sponsored_terms_are_immutable_and_load_as_a_complete_manifest() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let Fixture {
        database,
        context,
        allocation,
    } = fixture;

    assert!(matches!(
        load_sponsored_allocation_manifest(
            &database.pool,
            &context,
            &allocation.provider_customer_id,
            &allocation.subscription_id,
            SponsoredManifestLoadLimits::default(),
        )
        .await,
        Err(SponsoredManifestLoadError::Corrupt(_))
    ));

    let mut tx = database.pool.begin().await.expect("begin");
    let first =
        record_sponsored_allocation_term(&mut tx, &context, &allocation, "price_sponsored_monthly")
            .await
            .expect("record term");
    assert_eq!(
        first.disposition,
        SponsoredAllocationTermDisposition::Recorded
    );
    let replay =
        record_sponsored_allocation_term(&mut tx, &context, &allocation, "price_sponsored_monthly")
            .await
            .expect("replay term");
    assert_eq!(
        replay.disposition,
        SponsoredAllocationTermDisposition::AlreadyRecorded
    );
    tx.commit().await.expect("commit");

    let manifest = load_sponsored_allocation_manifest(
        &database.pool,
        &context,
        &allocation.provider_customer_id,
        &allocation.subscription_id,
        SponsoredManifestLoadLimits::default(),
    )
    .await
    .expect("load manifest");
    assert_eq!(manifest.allocations().len(), 1);
    let interval = &manifest.allocations()[0];
    assert_eq!(interval.source_id(), allocation.source_id);
    assert_eq!(interval.beneficiary_id(), allocation.beneficiary_id);
    assert_eq!(interval.provider_item_id(), allocation.provider_item_id);
    assert_eq!(interval.price_id(), "price_sponsored_monthly");
    assert_eq!(interval.effective_from(), 100);
    assert_eq!(interval.effective_until(), Some(200));

    let mut conflict = database.pool.begin().await.expect("begin conflict");
    assert!(matches!(
        record_sponsored_allocation_term(
            &mut conflict,
            &context,
            &allocation,
            "price_sponsored_annual",
        )
        .await,
        Err(SponsoredAllocationTermStoreError::TermConflict)
    ));
    conflict.rollback().await.expect("rollback conflict");

    sqlx::query(
        "UPDATE cloud_provider_payers SET provider_account_id = 'tampered-account' WHERE payer_id = $1",
    )
    .bind(&allocation.payer_id)
    .execute(&database.pool)
    .await
    .expect("tamper payer");
    let mut payer_conflict = database.pool.begin().await.expect("begin payer conflict");
    assert!(matches!(
        record_sponsored_allocation_term(
            &mut payer_conflict,
            &context,
            &allocation,
            "price_sponsored_monthly",
        )
        .await,
        Err(SponsoredAllocationTermStoreError::AllocationConflict)
    ));
    payer_conflict
        .rollback()
        .await
        .expect("rollback payer conflict");
    assert!(matches!(
        load_sponsored_allocation_manifest(
            &database.pool,
            &context,
            &allocation.provider_customer_id,
            &allocation.subscription_id,
            SponsoredManifestLoadLimits::default(),
        )
        .await,
        Err(SponsoredManifestLoadError::Corrupt(_))
    ));
    sqlx::query("UPDATE cloud_provider_payers SET provider_account_id = $1 WHERE payer_id = $2")
        .bind(&context.account_id)
        .bind(&allocation.payer_id)
        .execute(&database.pool)
        .await
        .expect("restore payer");

    sqlx::query(
        "UPDATE cloud_provider_sponsored_allocation_terms \
         SET effective_until = 201 WHERE allocation_id = $1",
    )
    .bind(&allocation.allocation_id)
    .execute(&database.pool)
    .await
    .expect("tamper term");
    assert!(matches!(
        load_sponsored_allocation_manifest(
            &database.pool,
            &context,
            &allocation.provider_customer_id,
            &allocation.subscription_id,
            SponsoredManifestLoadLimits::default(),
        )
        .await,
        Err(SponsoredManifestLoadError::Corrupt(_))
    ));
    database.cleanup().await;
}

#[tokio::test]
async fn sponsored_manifest_load_limits_fail_closed_before_returning_a_prefix() {
    let Some(fixture) = Fixture::create().await else {
        return;
    };
    let Fixture {
        database,
        context,
        allocation,
    } = fixture;
    let mut tx = database.pool.begin().await.expect("begin");
    record_sponsored_allocation_term(&mut tx, &context, &allocation, "price_sponsored_monthly")
        .await
        .expect("record term");
    tx.commit().await.expect("commit");
    assert!(matches!(
        load_sponsored_allocation_manifest(
            &database.pool,
            &context,
            &allocation.provider_customer_id,
            &allocation.subscription_id,
            SponsoredManifestLoadLimits {
                max_rows: 0,
                max_bytes: 1024,
            },
        )
        .await,
        Err(SponsoredManifestLoadError::InvalidLimits)
    ));
    assert!(matches!(
        load_sponsored_allocation_manifest(
            &database.pool,
            &context,
            &allocation.provider_customer_id,
            &allocation.subscription_id,
            SponsoredManifestLoadLimits {
                max_rows: 1,
                max_bytes: 1,
            },
        )
        .await,
        Err(SponsoredManifestLoadError::BoundExceeded { limit: 1 })
    ));
    database.cleanup().await;
}
