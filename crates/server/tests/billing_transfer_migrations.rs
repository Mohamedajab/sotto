//! Populated upgrade coverage for transfer execution evidence.

use sotto_server::db;

#[path = "support/migrations.rs"]
mod migrations;
use migrations::{migrator_before, DisposableDatabase};

#[tokio::test]
async fn unbound_live_transfer_intents_are_parked_during_upgrade() {
    let Some(database) = DisposableDatabase::create().await else {
        return;
    };
    migrator_before(56)
        .run(&database.pool)
        .await
        .expect("apply schema before transfer execution migration");

    for user_id in ["transfer-upgrade-actor", "transfer-upgrade-counterparty"] {
        sqlx::query(
            "INSERT INTO users (id, oauth_provider, oauth_subject) VALUES ($1, 'migration', $1)",
        )
        .bind(user_id)
        .execute(&database.pool)
        .await
        .expect("insert transfer participant");
    }
    sqlx::query(
        "INSERT INTO organizations (id, enc_name) VALUES ('transfer-upgrade-org', '\\x00')",
    )
    .execute(&database.pool)
    .await
    .expect("insert transfer destination");

    sqlx::query(
        "INSERT INTO billing_transfer_intents \
         (transfer_id, actor_user_id, counterparty_user_id, beneficiary_id, source_kind, \
          destination_kind, destination_organization_id, offer, quote_version, \
          quote_expires_at_epoch, effective_from, idempotency_key, request_hash, \
          provider_idempotency_key, state) \
         VALUES ('transfer:upgrade-live', 'transfer-upgrade-actor', \
                 'transfer-upgrade-counterparty', 'transfer-upgrade-actor', 'personal', \
                 'sponsor', 'transfer-upgrade-org', 'standard_monthly', 1, 2000000000, \
                 2000000100, 'upgrade-live', 'upgrade-live-hash', 'upgrade-live-provider', \
                 'awaiting_consent')",
    )
    .execute(&database.pool)
    .await
    .expect("insert pre-binding live intent");
    sqlx::query(
        "INSERT INTO billing_transfer_intents \
         (transfer_id, actor_user_id, counterparty_user_id, beneficiary_id, source_kind, \
          destination_kind, destination_organization_id, offer, quote_version, \
          quote_expires_at_epoch, effective_from, idempotency_key, request_hash, \
          provider_idempotency_key, state, result_code) \
         VALUES ('transfer:upgrade-completed', 'transfer-upgrade-actor', \
                 'transfer-upgrade-counterparty', 'transfer-upgrade-actor', 'personal', \
                 'sponsor', 'transfer-upgrade-org', 'standard_monthly', 1, 2000000000, \
                 2000000100, 'upgrade-completed', 'upgrade-completed-hash', \
                 'upgrade-completed-provider', 'completed', 'completed')",
    )
    .execute(&database.pool)
    .await
    .expect("insert historical completed intent");

    db::migrate(&database.pool)
        .await
        .expect("apply transfer execution migration");

    let live: (String, Option<String>) = sqlx::query_as(
        "SELECT state, result_code FROM billing_transfer_intents \
         WHERE transfer_id = 'transfer:upgrade-live'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("read upgraded live intent");
    assert_eq!(
        live,
        ("failed".into(), Some("source_binding_unavailable".into()))
    );

    let completed: (String, Option<String>) = sqlx::query_as(
        "SELECT state, result_code FROM billing_transfer_intents \
         WHERE transfer_id = 'transfer:upgrade-completed'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("read historical completed intent");
    assert_eq!(completed, ("completed".into(), Some("completed".into())));
    database.cleanup().await;
}
