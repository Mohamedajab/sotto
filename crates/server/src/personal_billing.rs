//! Durable state for a person's hosted Cloud subscription.
//!
//! This table is deliberately separate from organisation tiers. A personal checkout can be
//! pending while Stripe is still collecting payment, and a paid term remains readable after a
//! cancellation request. The module owns only database state; provider verification stays in the
//! billing webhook path.

use sqlx::{Postgres, Row, Transaction};
use thiserror::Error;

use crate::billing_catalogue::BillingOffer;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersonalBillingState {
    Pending,
    Active,
    PastDue,
    Unpaid,
    Canceled,
}

impl PersonalBillingState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Active => "active",
            Self::PastDue => "past_due",
            Self::Unpaid => "unpaid",
            Self::Canceled => "canceled",
        }
    }

    fn parse(value: &str) -> Result<Self, PersonalBillingError> {
        match value {
            "pending" => Ok(Self::Pending),
            "active" => Ok(Self::Active),
            "past_due" => Ok(Self::PastDue),
            "unpaid" => Ok(Self::Unpaid),
            "canceled" => Ok(Self::Canceled),
            _ => Err(PersonalBillingError::CorruptState),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersonalBillingAccount {
    pub user_id: String,
    pub operation_id: String,
    pub offer: BillingOffer,
    pub stripe_customer_id: Option<String>,
    pub stripe_subscription_id: Option<String>,
    pub state: PersonalBillingState,
    pub paid_through_epoch: Option<i64>,
    pub cancel_at_period_end: bool,
}

#[derive(Debug, Error)]
pub enum PersonalBillingError {
    #[error("personal billing account already exists")]
    AccountExists,
    #[error("personal billing account is corrupt")]
    CorruptState,
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
}

/// Reserve the one personal billing account row for an operation. Replaying the same operation is
/// idempotent; a different operation cannot create a second personal subscription.
pub async fn begin_account(
    tx: &mut Transaction<'_, Postgres>,
    user_id: &str,
    operation_id: &str,
    offer: BillingOffer,
) -> Result<(), PersonalBillingError> {
    let existing = sqlx::query(
        "SELECT operation_id, offer, state FROM billing_personal_accounts \
         WHERE user_id = $1 FOR UPDATE",
    )
    .bind(user_id)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(row) = existing {
        let existing_operation: String = row.try_get("operation_id")?;
        let existing_offer: String = row.try_get("offer")?;
        let state: String = row.try_get("state")?;
        let same_pending = existing_operation == operation_id
            && existing_offer == offer.as_str()
            && state == PersonalBillingState::Pending.as_str();
        if same_pending {
            return Ok(());
        }
        return Err(PersonalBillingError::AccountExists);
    }
    sqlx::query(
        "INSERT INTO billing_personal_accounts (user_id, operation_id, offer) \
         VALUES ($1, $2, $3)",
    )
    .bind(user_id)
    .bind(operation_id)
    .bind(offer.as_str())
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn load_account(
    tx: &mut Transaction<'_, Postgres>,
    user_id: &str,
) -> Result<Option<PersonalBillingAccount>, PersonalBillingError> {
    let row = sqlx::query(
        "SELECT user_id, operation_id, offer, stripe_customer_id, stripe_subscription_id, \
                state, paid_through_epoch, cancel_at_period_end \
         FROM billing_personal_accounts WHERE user_id = $1",
    )
    .bind(user_id)
    .fetch_optional(&mut **tx)
    .await?;
    row.map(account_from_row).transpose()
}

pub async fn record_checkout_url(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: &str,
    checkout_url: &str,
) -> Result<(), PersonalBillingError> {
    sqlx::query(
        "UPDATE billing_operations SET provider_checkout_url = $2, updated_at = now() \
         WHERE operation_id = $1",
    )
    .bind(operation_id)
    .bind(checkout_url)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn account_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<PersonalBillingAccount, PersonalBillingError> {
    let offer = match row.try_get::<String, _>("offer")?.as_str() {
        "standard_monthly" => BillingOffer::StandardMonthly,
        "standard_annual" => BillingOffer::StandardAnnual,
        "founding_monthly" => BillingOffer::FoundingMonthly,
        "founding_annual" => BillingOffer::FoundingAnnual,
        _ => return Err(PersonalBillingError::CorruptState),
    };
    Ok(PersonalBillingAccount {
        user_id: row.try_get("user_id")?,
        operation_id: row.try_get("operation_id")?,
        offer,
        stripe_customer_id: row.try_get("stripe_customer_id")?,
        stripe_subscription_id: row.try_get("stripe_subscription_id")?,
        state: PersonalBillingState::parse(&row.try_get::<String, _>("state")?)?,
        paid_through_epoch: row.try_get("paid_through_epoch")?,
        cancel_at_period_end: row.try_get("cancel_at_period_end")?,
    })
}
