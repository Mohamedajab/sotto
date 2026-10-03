-- A sponsored Stripe subscription item can cover several named beneficiaries at once.
-- Allocation references and coverage sources remain unique; the provider item itself is not.

ALTER TABLE cloud_provider_allocations
    DROP CONSTRAINT cloud_provider_allocations_provider_namespace_provider_account_id_provider_environment_provider_subscription_id_provider_item_id_key;

CREATE INDEX cloud_provider_allocations_provider_item_idx
    ON cloud_provider_allocations (
        provider_namespace,
        provider_account_id,
        provider_environment,
        provider_subscription_id,
        provider_item_id,
        effective_from
    );
