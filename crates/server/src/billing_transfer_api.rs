//! Authenticated, ships-dark API for quoting and consenting to payer transfers.
//!
//! The API exposes only server-derived local billing terms. Provider execution is not mounted
//! here; even an accepted intent remains inert until a separate recovery-capable worker ships.
//! A distinct counterparty must accept an invitation. One person may start a transfer directly
//! only when the server verifies that they have authority for both payer sides.

use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::auth::AuthUser;
use crate::billing_catalogue::BillingOffer;
use crate::billing_transfers::{
    self, BeginTransfer, TransferDraft, TransferIntent, TransferPayer, TransferRequest,
    TransferState, TRANSFER_QUOTE_SECONDS, TRANSFER_QUOTE_VERSION,
};
use crate::error::{Error, Result};
use crate::state::AppState;

const MAX_TRANSFER_PAGE_SIZE: i64 = 50;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/billing/transfers/quote", post(quote_transfer))
        .route(
            "/billing/transfers",
            get(list_transfers).post(begin_transfer),
        )
        .route("/billing/transfers/{transfer_id}", get(get_transfer))
        .route(
            "/billing/transfers/{transfer_id}/accept",
            post(accept_transfer),
        )
        .route(
            "/billing/transfers/{transfer_id}/withdraw",
            post(withdraw_transfer),
        )
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferDraftRequest {
    beneficiary_id: String,
    counterparty_user_id: String,
    destination_kind: String,
    #[serde(default)]
    destination_organization_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BeginTransferRequest {
    beneficiary_id: String,
    counterparty_user_id: String,
    destination_kind: String,
    #[serde(default)]
    destination_organization_id: Option<String>,
    quote_id: String,
    quote_version: i64,
    quote_expires_at_epoch: i64,
    idempotency_key: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct TransferListQuery {
    #[serde(default)]
    before: Option<String>,
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct TransferQuoteView {
    quote_id: String,
    quote_version: i64,
    quote_expires_at_epoch: i64,
    beneficiary_id: String,
    source_kind: String,
    source_organization_id: Option<String>,
    destination_kind: String,
    destination_organization_id: Option<String>,
    offer: String,
    amount_pence: i64,
    currency: &'static str,
    interval: &'static str,
    effective_from: i64,
    credit_pence: i64,
    founding: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct TransferListView {
    transfers: Vec<TransferView>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
struct TransferView {
    transfer_id: String,
    beneficiary_id: String,
    participant_role: &'static str,
    source_kind: String,
    source_organization_id: Option<String>,
    destination_kind: String,
    destination_organization_id: Option<String>,
    offer: String,
    amount_pence: i64,
    currency: &'static str,
    interval: &'static str,
    effective_from: i64,
    quote_expires_at_epoch: i64,
    state: String,
    result_code: Option<&'static str>,
}

async fn quote_transfer(
    State(state): State<AppState>,
    user: AuthUser,
    Json(body): Json<TransferDraftRequest>,
) -> Result<Json<TransferQuoteView>> {
    let now = current_epoch()?;
    let expires = now
        .checked_add(TRANSFER_QUOTE_SECONDS)
        .ok_or_else(|| Error::Internal("transfer quote clock overflow".into()))?;
    let draft = transfer_draft(
        body,
        TRANSFER_QUOTE_VERSION,
        expires,
        "quote-preview".into(),
    )?;
    let mut tx = state.pool.begin().await?;
    let request =
        billing_transfers::build_transfer_request(&mut tx, &user.user_id, &draft, now).await?;
    let view = quote_view(&request);
    tx.rollback().await?;
    Ok(Json(view))
}

async fn begin_transfer(
    State(state): State<AppState>,
    user: AuthUser,
    Json(body): Json<BeginTransferRequest>,
) -> Result<Json<TransferView>> {
    validate_idempotency_key(&body.idempotency_key)?;
    validate_quote_id(&body.quote_id)?;
    let now = current_epoch()?;
    let mut tx = state.pool.begin().await?;
    if let Some(existing) =
        billing_transfers::find_by_idempotency(&mut tx, &user.user_id, &body.idempotency_key)
            .await?
    {
        if !existing_matches_body(&existing, &body)
            || quote_id_from_intent(&existing) != body.quote_id
        {
            return Err(Error::Conflict(
                "transfer idempotency key conflicts with a different request".into(),
            ));
        }
        let view = transfer_view(existing, &user.user_id, now);
        tx.commit().await?;
        return Ok(Json(view));
    }

    let draft = transfer_draft(
        TransferDraftRequest {
            beneficiary_id: body.beneficiary_id,
            counterparty_user_id: body.counterparty_user_id,
            destination_kind: body.destination_kind,
            destination_organization_id: body.destination_organization_id,
        },
        body.quote_version,
        body.quote_expires_at_epoch,
        body.idempotency_key,
    )?;
    let request =
        billing_transfers::build_transfer_request(&mut tx, &user.user_id, &draft, now).await?;
    if quote_id(&request) != body.quote_id {
        return Err(Error::Conflict(
            "transfer quote changed; request a fresh quote".into(),
        ));
    }
    let outcome = billing_transfers::begin_transfer(&mut tx, &request, now).await?;
    let intent = match outcome {
        BeginTransfer::Created(intent) | BeginTransfer::AlreadyExists(intent) => intent,
    };
    let view = transfer_view(intent, &user.user_id, now);
    tx.commit().await?;
    Ok(Json(view))
}

async fn accept_transfer(
    State(state): State<AppState>,
    user: AuthUser,
    Path(transfer_id): Path<String>,
) -> Result<Json<TransferView>> {
    let now = current_epoch()?;
    let mut tx = state.pool.begin().await?;
    let intent =
        billing_transfers::accept_transfer(&mut tx, &transfer_id, &user.user_id, now).await?;
    let view = transfer_view(intent, &user.user_id, now);
    tx.commit().await?;
    Ok(Json(view))
}

async fn withdraw_transfer(
    State(state): State<AppState>,
    user: AuthUser,
    Path(transfer_id): Path<String>,
) -> Result<Json<TransferView>> {
    let now = current_epoch()?;
    let mut tx = state.pool.begin().await?;
    let intent = billing_transfers::withdraw_transfer(&mut tx, &transfer_id, &user.user_id).await?;
    let view = transfer_view(intent, &user.user_id, now);
    tx.commit().await?;
    Ok(Json(view))
}

async fn get_transfer(
    State(state): State<AppState>,
    user: AuthUser,
    Path(transfer_id): Path<String>,
) -> Result<Json<TransferView>> {
    let now = current_epoch()?;
    let intent = billing_transfers::load_for_participant(&state.pool, &transfer_id, &user.user_id)
        .await?
        .ok_or_else(|| Error::NotFound("transfer not found".into()))?;
    Ok(Json(transfer_view(intent, &user.user_id, now)))
}

async fn list_transfers(
    State(state): State<AppState>,
    user: AuthUser,
    Query(query): Query<TransferListQuery>,
) -> Result<Json<TransferListView>> {
    let limit = query.limit.unwrap_or(20);
    if !(1..=MAX_TRANSFER_PAGE_SIZE).contains(&limit) {
        return Err(Error::BadRequest(
            "transfer list limit must be between 1 and 50".into(),
        ));
    }
    if query.before.as_deref().is_some_and(|cursor| {
        cursor.trim().is_empty()
            || cursor.len() > 128
            || !cursor.bytes().all(|byte| byte.is_ascii_graphic())
    }) {
        return Err(Error::BadRequest("invalid transfer cursor".into()));
    }
    let now = current_epoch()?;
    let rows = billing_transfers::list_for_participant(
        &state.pool,
        &user.user_id,
        query.before.as_deref(),
        limit + 1,
    )
    .await?;
    let has_more = rows.len() as i64 > limit;
    let transfers = rows
        .into_iter()
        .take(limit as usize)
        .map(|intent| transfer_view(intent, &user.user_id, now))
        .collect::<Vec<_>>();
    let next_cursor = has_more
        .then(|| transfers.last().map(|item| item.transfer_id.clone()))
        .flatten();
    Ok(Json(TransferListView {
        transfers,
        next_cursor,
    }))
}

fn transfer_draft(
    body: TransferDraftRequest,
    quote_version: i64,
    quote_expires_at_epoch: i64,
    idempotency_key: String,
) -> Result<TransferDraft> {
    for (value, field) in [
        (body.beneficiary_id.as_str(), "beneficiary"),
        (body.counterparty_user_id.as_str(), "counterparty"),
    ] {
        if value.trim().is_empty()
            || value.len() > 255
            || !value.bytes().all(|byte| byte.is_ascii_graphic())
        {
            return Err(Error::BadRequest(format!("invalid transfer {field}")));
        }
    }
    let destination_kind = TransferPayer::parse(&body.destination_kind)?;
    if (destination_kind == TransferPayer::Personal) != body.destination_organization_id.is_none() {
        return Err(Error::BadRequest("invalid destination payer".into()));
    }
    if body
        .destination_organization_id
        .as_deref()
        .is_some_and(|value| {
            value.trim().is_empty()
                || value.len() > 255
                || !value.bytes().all(|byte| byte.is_ascii_graphic())
        })
    {
        return Err(Error::BadRequest("invalid destination organisation".into()));
    }
    Ok(TransferDraft {
        counterparty_user_id: body.counterparty_user_id,
        beneficiary_id: body.beneficiary_id,
        destination_kind,
        destination_organization_id: body.destination_organization_id,
        quote_version,
        quote_expires_at_epoch,
        idempotency_key,
    })
}

fn quote_view(request: &TransferRequest) -> TransferQuoteView {
    TransferQuoteView {
        quote_id: quote_id(request),
        quote_version: request.quote_version,
        quote_expires_at_epoch: request.quote_expires_at_epoch,
        beneficiary_id: request.beneficiary_id.clone(),
        source_kind: request.source_kind.as_str().into(),
        source_organization_id: request.source_organization_id.clone(),
        destination_kind: request.destination_kind.as_str().into(),
        destination_organization_id: request.destination_organization_id.clone(),
        offer: request.offer.as_str().into(),
        amount_pence: request.offer.expected_amount_pence(),
        currency: "gbp",
        interval: interval_label(request.offer),
        effective_from: request.effective_from,
        credit_pence: 0,
        founding: crate::founding_allocator::FoundingOffer::from_billing_offer(request.offer)
            .is_some(),
    }
}

fn transfer_view(intent: TransferIntent, user_id: &str, now: i64) -> TransferView {
    let expired =
        intent.state == TransferState::AwaitingConsent && intent.quote_expires_at_epoch <= now;
    let state = if expired {
        "expired"
    } else {
        intent.state.as_str()
    };
    let result_code = if expired {
        Some("consent_expired")
    } else {
        match intent.result_code.as_deref() {
            Some("consent_expired") => Some("consent_expired"),
            Some("consent_withdrawn") => Some("consent_withdrawn"),
            Some("provider_rejected") => Some("provider_rejected"),
            Some("source_adjustment_failed") => Some("source_adjustment_failed"),
            Some(_) => Some("other"),
            None => None,
        }
    };
    TransferView {
        transfer_id: intent.transfer_id,
        beneficiary_id: intent.beneficiary_id,
        participant_role: if intent.actor_user_id == user_id {
            "initiator"
        } else {
            "counterparty"
        },
        source_kind: intent.source_kind.as_str().into(),
        source_organization_id: intent.source_organization_id,
        destination_kind: intent.destination_kind.as_str().into(),
        destination_organization_id: intent.destination_organization_id,
        offer: intent.offer.as_str().into(),
        amount_pence: intent.offer.expected_amount_pence(),
        currency: "gbp",
        interval: interval_label(intent.offer),
        effective_from: intent.effective_from,
        quote_expires_at_epoch: intent.quote_expires_at_epoch,
        state: state.into(),
        result_code,
    }
}

fn interval_label(offer: BillingOffer) -> &'static str {
    match offer.expected_interval() {
        crate::billing_catalogue::BillingInterval::Month => "month",
        crate::billing_catalogue::BillingInterval::Year => "year",
    }
}

fn quote_id(request: &TransferRequest) -> String {
    let version = request.quote_version.to_string();
    let expires = request.quote_expires_at_epoch.to_string();
    let effective = request.effective_from.to_string();
    let fields = [
        "sotto-transfer-quote-v1",
        request.actor_user_id.as_str(),
        request.counterparty_user_id.as_str(),
        request.beneficiary_id.as_str(),
        request.source_kind.as_str(),
        request.source_organization_id.as_deref().unwrap_or(""),
        request.source_provider_subscription_id.as_str(),
        request.destination_kind.as_str(),
        request.destination_organization_id.as_deref().unwrap_or(""),
        request.offer.as_str(),
        version.as_str(),
        expires.as_str(),
        effective.as_str(),
    ];
    transfer_quote_fingerprint(&fields)
}

fn quote_id_from_intent(intent: &TransferIntent) -> String {
    let version = intent.quote_version.to_string();
    let expires = intent.quote_expires_at_epoch.to_string();
    let effective = intent.effective_from.to_string();
    let mut fields = vec![
        "sotto-transfer-quote-v1",
        intent.actor_user_id.as_str(),
        intent.counterparty_user_id.as_str(),
        intent.beneficiary_id.as_str(),
        intent.source_kind.as_str(),
        intent.source_organization_id.as_deref().unwrap_or(""),
    ];
    // Intents created before source binding used the original fingerprint without this field.
    // Preserve that quote only for idempotent reads of historical, now non-executable rows.
    if let Some(subscription_id) = intent.source_provider_subscription_id.as_deref() {
        fields.push(subscription_id);
    }
    fields.extend([
        intent.destination_kind.as_str(),
        intent.destination_organization_id.as_deref().unwrap_or(""),
        intent.offer.as_str(),
        version.as_str(),
        expires.as_str(),
        effective.as_str(),
    ]);
    transfer_quote_fingerprint(&fields)
}

fn transfer_quote_fingerprint(fields: &[&str]) -> String {
    let mut encoded = Vec::new();
    for field in fields {
        encoded.extend_from_slice(&(field.len() as u64).to_be_bytes());
        encoded.extend_from_slice(field.as_bytes());
    }
    let digest = Sha256::digest(encoded);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn existing_matches_body(intent: &TransferIntent, body: &BeginTransferRequest) -> bool {
    intent.beneficiary_id == body.beneficiary_id
        && intent.counterparty_user_id == body.counterparty_user_id
        && intent.destination_kind.as_str() == body.destination_kind
        && intent.destination_organization_id == body.destination_organization_id
        && intent.quote_version == body.quote_version
        && intent.quote_expires_at_epoch == body.quote_expires_at_epoch
}

fn validate_quote_id(value: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::BadRequest("invalid transfer quote".into()));
    }
    Ok(())
}

fn validate_idempotency_key(value: &str) -> Result<()> {
    if value.trim().is_empty()
        || value.len() > 128
        || !value.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(Error::BadRequest("invalid transfer idempotency key".into()));
    }
    Ok(())
}

fn current_epoch() -> Result<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .map_err(|_| Error::Internal("system clock before unix epoch".into()))
}

#[cfg(test)]
mod tests {
    use super::{
        quote_id, quote_id_from_intent, transfer_quote_fingerprint, validate_idempotency_key,
        validate_quote_id,
    };
    use crate::billing_catalogue::BillingOffer;
    use crate::billing_transfers::{TransferIntent, TransferPayer, TransferRequest, TransferState};

    fn request() -> TransferRequest {
        TransferRequest {
            actor_user_id: "actor".into(),
            counterparty_user_id: "counterparty".into(),
            beneficiary_id: "beneficiary".into(),
            source_kind: TransferPayer::Personal,
            source_organization_id: None,
            source_provider_subscription_id: "sub_source".into(),
            destination_kind: TransferPayer::Sponsor,
            destination_organization_id: Some("org".into()),
            offer: BillingOffer::StandardMonthly,
            quote_version: 1,
            quote_expires_at_epoch: 2_000,
            effective_from: 3_000,
            effective_until: None,
            idempotency_key: "request".into(),
        }
    }

    #[test]
    fn quote_fingerprint_binds_every_quote_term_but_not_the_idempotency_key() {
        let original = request();
        let mut changed = original.clone();
        changed.idempotency_key = "other".into();
        assert_eq!(quote_id(&original), quote_id(&changed));

        changed.offer = BillingOffer::StandardAnnual;
        assert_ne!(quote_id(&original), quote_id(&changed));
        changed = original.clone();
        changed.effective_from += 1;
        assert_ne!(quote_id(&original), quote_id(&changed));
        changed = original.clone();
        changed.counterparty_user_id = "other".into();
        assert_ne!(quote_id(&original), quote_id(&changed));
        changed = original.clone();
        changed.source_kind = TransferPayer::Sponsor;
        changed.source_organization_id = Some("source org".into());
        assert_ne!(quote_id(&original), quote_id(&changed));
        changed = original.clone();
        changed.source_provider_subscription_id = "sub_changed".into();
        assert_ne!(quote_id(&original), quote_id(&changed));
        changed = original.clone();
        changed.destination_organization_id = Some("different org".into());
        assert_ne!(quote_id(&original), quote_id(&changed));
        changed = original.clone();
        changed.quote_expires_at_epoch += 1;
        assert_ne!(quote_id(&original), quote_id(&changed));
        changed = original.clone();
        changed.quote_version += 1;
        assert_ne!(quote_id(&original), quote_id(&changed));
    }

    #[test]
    fn quote_ids_and_idempotency_keys_are_bounded() {
        assert!(validate_quote_id(&"a".repeat(64)).is_ok());
        assert!(validate_quote_id("short").is_err());
        assert!(validate_idempotency_key("web-req-1").is_ok());
        assert!(validate_idempotency_key("\n").is_err());
        assert!(validate_idempotency_key(&"x".repeat(129)).is_err());
    }

    #[test]
    fn historical_intents_keep_their_original_quote_fingerprint() {
        let intent = TransferIntent {
            transfer_id: "transfer:legacy".into(),
            actor_user_id: "actor".into(),
            counterparty_user_id: "counterparty".into(),
            beneficiary_id: "beneficiary".into(),
            source_kind: TransferPayer::Personal,
            source_organization_id: None,
            destination_kind: TransferPayer::Sponsor,
            destination_organization_id: Some("org".into()),
            offer: BillingOffer::StandardMonthly,
            quote_version: 1,
            quote_expires_at_epoch: 2_000,
            request_hash: "request-hash".into(),
            idempotency_key: "request".into(),
            provider_idempotency_key: "provider-key".into(),
            effective_from: 3_000,
            effective_until: None,
            destination_operation_id: None,
            source_operation_id: None,
            destination_provider_subscription_id: None,
            source_provider_subscription_id: None,
            destination_payment_reference: None,
            source_adjustment_reference: None,
            source_restoration_reference: None,
            founding_award_id: None,
            execution_lease_token: None,
            execution_lease_until_epoch: None,
            execution_attempt_count: 0,
            destination_customer_id: None,
            destination_schedule_id: None,
            source_schedule_id: None,
            destination_setup_session_id: None,
            destination_setup_intent_id: None,
            last_execution_error_code: None,
            state: TransferState::Failed,
            result_code: Some("source_binding_unavailable".into()),
        };
        let version = intent.quote_version.to_string();
        let expires = intent.quote_expires_at_epoch.to_string();
        let effective = intent.effective_from.to_string();
        let legacy_fields = [
            "sotto-transfer-quote-v1",
            "actor",
            "counterparty",
            "beneficiary",
            "personal",
            "",
            "sponsor",
            "org",
            "standard_monthly",
            version.as_str(),
            expires.as_str(),
            effective.as_str(),
        ];
        assert_eq!(
            quote_id_from_intent(&intent),
            transfer_quote_fingerprint(&legacy_fields)
        );
    }
}
