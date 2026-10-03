-- A sponsored Stripe subscription item can cover several named beneficiaries at once.
-- Allocation references and coverage sources remain unique; the provider item itself is not.

ALTER TABLE cloud_provider_allocations
    DROP CONSTRAINT cloud_provider_allocations_provider_namespace_provider_account_;

-- Personal allocations still represent one quantity-one Stripe item. Only sponsor rows may
-- share an item, because the sponsored manifest is the boundary that attributes its quantity.
CREATE UNIQUE INDEX cloud_provider_allocations_personal_provider_item_key
    ON cloud_provider_allocations (
        provider_namespace,
        provider_account_id,
        provider_environment,
        provider_subscription_id,
        provider_item_id
    )
    WHERE payer_kind = 'personal';

CREATE INDEX cloud_provider_allocations_provider_item_idx
    ON cloud_provider_allocations (
        provider_namespace,
        provider_account_id,
        provider_environment,
        provider_subscription_id,
        provider_item_id,
        effective_from
    );
