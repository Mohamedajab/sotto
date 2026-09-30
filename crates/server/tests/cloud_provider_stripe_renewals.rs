//! Loopback-backed tests for signed personal renewal failure evidence.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use axum::routing::any;
use axum::Router;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use sotto_server::cloud_provider::{PayerKind, ProviderEnvironment};
use sotto_server::cloud_provider_stripe::{
    StripeAllocationBinding, StripeContractError, StripeCoverageConfig,
};
use sotto_server::cloud_provider_stripe_http::{
    StripePersonalInvoiceHistory, StripePersonalInvoiceHistoryResult, StripeReadClient,
    StripeReadError, StripeReadLimits, StripeRenewalCurrentState, StripeRenewalNeedsEvidence,
    StripeRenewalObservationResult,
};
use sotto_server::cloud_provider_stripe_renewals::{
    decode_personal_renewal_failure, StripeRenewalFailureNeedsEvidence, StripeRenewalFailureResult,
};
use tokio::net::TcpListener;
use url::Url;

const SECRET: &str = "whsec_renewal_test";
const NOW: i64 = 3_100;

#[derive(Clone)]
struct MockState {
    responses: Arc<Mutex<HashMap<String, Vec<Value>>>>,
    requests: Arc<Mutex<Vec<String>>>,
}

struct MockServer {
    origin: Url,
    task: tokio::task::JoinHandle<()>,
    requests: Arc<Mutex<Vec<String>>>,
}

impl MockServer {
    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handler(State(state): State<MockState>, request: Request<Body>) -> Response<Body> {
    let path = request.uri().path().to_owned();
    state
        .requests
        .lock()
        .unwrap()
        .push(format!("{} {}", request.method(), request.uri()));
    let value = state
        .responses
        .lock()
        .unwrap()
        .get_mut(&path)
        .and_then(|responses| (!responses.is_empty()).then(|| responses.remove(0)))
        .unwrap_or_else(|| json!({"error":"unexpected request"}));
    let status = if value.get("error").is_some() {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::OK
    };
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&value).unwrap()))
        .unwrap()
}

async fn mock_server(responses: HashMap<String, Vec<Value>>) -> MockServer {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let state = MockState {
        responses: Arc::new(Mutex::new(responses)),
        requests: Arc::clone(&requests),
    };
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().fallback(any(handler)).with_state(state);
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    MockServer {
        origin: Url::parse(&format!("http://{address}/")).unwrap(),
        task,
        requests,
    }
}

fn config() -> StripeCoverageConfig {
    StripeCoverageConfig::new(
        "acct_test_renewal",
        ProviderEnvironment::Test,
        "price_month",
        "price_year",
    )
    .unwrap()
}

fn binding() -> StripeAllocationBinding {
    StripeAllocationBinding::new(
        "alloc_renewal",
        "cus_renewal",
        "sub_renewal",
        "si_renewal",
        PayerKind::Personal,
    )
    .unwrap()
}

fn limits() -> StripeReadLimits {
    StripeReadLimits {
        request_timeout: std::time::Duration::from_secs(2),
        session_timeout: std::time::Duration::from_secs(5),
        max_response_bytes: 64 * 1024,
        max_total_response_bytes: 256 * 1024,
        max_pages: 8,
        max_requests: 32,
        max_records: 100,
        max_retries: 0,
        max_retry_after: std::time::Duration::from_millis(10),
    }
}

fn list(data: Vec<Value>, has_more: bool) -> Value {
    json!({"object":"list","data":data,"has_more":has_more})
}

fn paid_invoice() -> Value {
    json!({
        "id":"in_paid",
        "customer":"cus_renewal",
        "parent":{"type":"subscription_details","subscription_details":{"subscription":"sub_renewal"}},
        "status":"paid","currency":"gbp","amount_paid":299,"amount_due":299,
        "amount_overpaid":0,"amount_paid_off_stripe":0,"livemode":false,
        "metadata":{"sotto_allocation_reference":"alloc_renewal"}
    })
}

fn paid_line() -> Value {
    json!({
        "id":"il_paid","quantity":1,"livemode":false,
        "parent":{"type":"subscription_item_details","subscription_item_details":{
            "subscription":"sub_renewal","subscription_item":"si_renewal","proration":false
        }},
        "pricing":{"type":"price_details","price_details":{"price":"price_month"}},
        "period":{"start":1000,"end":2000}
    })
}

fn signed_failure() -> Value {
    json!({
        "id":"evt_failure_1","created":3000,"api_version":"2026-07-29.dahlia",
        "type":"invoice.payment_failed","livemode":false,
        "data":{"object":{
            "object":"invoice","id":"in_failed","customer":"cus_renewal",
            "parent":{"type":"subscription_details","subscription_details":{"subscription":"sub_renewal"}},
            "status":"open","billing_reason":"subscription_cycle",
            "collection_method":"charge_automatically","currency":"gbp",
            "amount_due":299,"amount_remaining":299,"amount_paid":0,
            "amount_overpaid":0,"amount_paid_off_stripe":0,"livemode":false,
            "metadata":{"sotto_allocation_reference":"alloc_renewal"},
            "lines":{"object":"list","has_more":false,"data":[{
                "id":"il_failed","invoice":"in_failed","livemode":false,"quantity":1,
                "parent":{"type":"subscription_item_details","subscription_item_details":{
                    "subscription":"sub_renewal","subscription_item":"si_renewal","proration":false
                }},
                "pricing":{"type":"price_details","price_details":{"price":"price_month"}},
                "period":{"start":2000,"end":3000}
            }]}
        }}
    })
}

fn signed(value: &Value, timestamp: i64) -> (Vec<u8>, String) {
    let payload = serde_json::to_vec(value).unwrap();
    let mut mac = Hmac::<Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(format!("{timestamp}.{}", String::from_utf8_lossy(&payload)).as_bytes());
    let digest = mac.finalize().into_bytes();
    let signature = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    (payload, format!("t={timestamp},v1={signature}"))
}

async fn history() -> StripePersonalInvoiceHistory {
    let mut responses = HashMap::new();
    responses.insert(
        "/v1/account".into(),
        vec![json!({"id":"acct_test_renewal","object":"account","livemode":false})],
    );
    responses.insert(
        "/v1/subscriptions/sub_renewal".into(),
        vec![
            json!({"id":"sub_renewal","customer":"cus_renewal","status":"active","livemode":false}),
        ],
    );
    responses.insert(
        "/v1/invoices".into(),
        vec![list(vec![paid_invoice()], false)],
    );
    responses.insert("/v1/invoices/in_paid".into(), vec![paid_invoice()]);
    responses.insert(
        "/v1/invoices/in_paid/lines".into(),
        vec![list(vec![paid_line()], false)],
    );
    responses.insert(
        "/v1/invoice_payments".into(),
        vec![list(
            vec![json!({
                "id":"inpay_paid","invoice":"in_paid","status":"paid","amount_paid":299,
                "amount_requested":299,"currency":"gbp","livemode":false,
                "payment":{"type":"payment_intent","payment_intent":"pi_paid"}
            })],
            false,
        )],
    );
    for path in ["/v1/refunds", "/v1/disputes", "/v1/credit_notes"] {
        responses.insert(path.into(), vec![list(Vec::new(), false)]);
    }
    let server = mock_server(responses).await;
    let client = StripeReadClient::for_test(
        "sk_test_renewal",
        &config(),
        server.origin.clone(),
        limits(),
    )
    .unwrap();
    let mut session = client.session();
    let result = client
        .personal_invoice_history(&mut session, &binding())
        .await
        .unwrap();
    let StripePersonalInvoiceHistoryResult::Observed(history) = result else {
        panic!("expected observed history");
    };
    history
}

#[tokio::test]
async fn links_failure_to_exact_paid_predecessor_and_keeps_event_identity_separate() {
    let history = history().await;
    let (raw, signature) = signed(&signed_failure(), NOW);
    let result = decode_personal_renewal_failure(
        &raw,
        &signature,
        SECRET,
        NOW,
        &config(),
        &binding(),
        &history,
    )
    .unwrap();
    let StripeRenewalFailureResult::Linked(evidence) = result else {
        panic!("expected linked failure");
    };
    assert_eq!(evidence.predecessor_invoice_id(), "in_paid");
    assert_eq!(evidence.predecessor_period_end(), 2000);
    assert_eq!(evidence.renewal_period_start(), 2000);
    assert_eq!(evidence.renewal_period_end(), 3000);
    assert_eq!(evidence.invoice_id(), "in_failed");
    assert_eq!(evidence.event_id(), "evt_failure_1");
    assert_ne!(evidence.renewal_id(), evidence.event_id());
}

#[tokio::test]
async fn bad_signature_is_reported_before_malformed_json() {
    let history = history().await;
    let result = decode_personal_renewal_failure(
        b"not json",
        "t=3100,v1=00",
        SECRET,
        NOW,
        &config(),
        &binding(),
        &history,
    );
    assert!(matches!(result, Err(StripeContractError::InvalidSignature)));

    let (raw, signature) = signed(&signed_failure(), NOW);
    assert!(matches!(
        decode_personal_renewal_failure(
            &raw,
            &signature,
            "   ",
            NOW,
            &config(),
            &binding(),
            &history,
        ),
        Err(StripeContractError::InvalidConfig("Stripe webhook secret"))
    ));
}

#[tokio::test]
async fn retries_keep_one_renewal_identity_but_preserve_event_ids() {
    let history = history().await;
    let first_payload = signed_failure();
    let (first_raw, first_signature) = signed(&first_payload, NOW);
    let first = decode_personal_renewal_failure(
        &first_raw,
        &first_signature,
        SECRET,
        NOW,
        &config(),
        &binding(),
        &history,
    )
    .unwrap();
    let StripeRenewalFailureResult::Linked(first) = first else {
        panic!("expected linked first attempt");
    };

    let mut retry_payload = first_payload;
    retry_payload["id"] = json!("evt_failure_retry");
    let (retry_raw, retry_signature) = signed(&retry_payload, NOW);
    let retry = decode_personal_renewal_failure(
        &retry_raw,
        &retry_signature,
        SECRET,
        NOW,
        &config(),
        &binding(),
        &history,
    )
    .unwrap();
    let StripeRenewalFailureResult::Linked(retry) = retry else {
        panic!("expected linked retry");
    };
    assert_eq!(first.renewal_id(), retry.renewal_id());
    assert_ne!(first.event_id(), retry.event_id());
    assert_eq!(first.renewal_period_start(), retry.renewal_period_start());
}

#[tokio::test]
async fn incomplete_or_prorated_lines_never_link() {
    let history = history().await;

    let mut zero_remaining = signed_failure();
    zero_remaining["data"]["object"]["amount_remaining"] = json!(0);
    let (raw, signature) = signed(&zero_remaining, NOW);
    assert!(matches!(
        decode_personal_renewal_failure(
            &raw,
            &signature,
            SECRET,
            NOW,
            &config(),
            &binding(),
            &history,
        ),
        Err(StripeContractError::InvalidField("amount_remaining"))
    ));

    let mut truncated = signed_failure();
    truncated["data"]["object"]["lines"]["has_more"] = json!(true);
    let (raw, signature) = signed(&truncated, NOW);
    assert_eq!(
        decode_personal_renewal_failure(
            &raw,
            &signature,
            SECRET,
            NOW,
            &config(),
            &binding(),
            &history,
        )
        .unwrap(),
        StripeRenewalFailureResult::NeedsEvidence(
            StripeRenewalFailureNeedsEvidence::TruncatedInvoiceLines
        )
    );

    let mut missing_proration = signed_failure();
    let details = &mut missing_proration["data"]["object"]["lines"]["data"][0]["parent"]
        ["subscription_item_details"];
    details.as_object_mut().unwrap().remove("proration");
    let (raw, signature) = signed(&missing_proration, NOW);
    assert_eq!(
        decode_personal_renewal_failure(
            &raw,
            &signature,
            SECRET,
            NOW,
            &config(),
            &binding(),
            &history,
        )
        .unwrap(),
        StripeRenewalFailureResult::NeedsEvidence(
            StripeRenewalFailureNeedsEvidence::MissingProrationProof
        )
    );

    let mut missing_invoice_reference = signed_failure();
    missing_invoice_reference["data"]["object"]["lines"]["data"][0]
        .as_object_mut()
        .unwrap()
        .remove("invoice");
    let (raw, signature) = signed(&missing_invoice_reference, NOW);
    assert!(matches!(
        decode_personal_renewal_failure(
            &raw,
            &signature,
            SECRET,
            NOW,
            &config(),
            &binding(),
            &history,
        ),
        Err(StripeContractError::MissingField("invoice"))
    ));
}

#[tokio::test]
async fn missing_exact_predecessor_does_not_create_partial_evidence() {
    let history = history().await;
    let mut failure = signed_failure();
    failure["data"]["object"]["lines"]["data"][0]["period"]["start"] = json!(2500);
    let (raw, signature) = signed(&failure, NOW);
    let result = decode_personal_renewal_failure(
        &raw,
        &signature,
        SECRET,
        NOW,
        &config(),
        &binding(),
        &history,
    )
    .unwrap();
    assert_eq!(
        result,
        StripeRenewalFailureResult::NeedsEvidence(
            StripeRenewalFailureNeedsEvidence::NoMatchingPaidPredecessor
        )
    );
}

async fn linked_failure(
) -> sotto_server::cloud_provider_stripe_renewals::StripeRenewalFailureEvidence {
    let history = history().await;
    let (raw, signature) = signed(&signed_failure(), NOW);
    let StripeRenewalFailureResult::Linked(evidence) = decode_personal_renewal_failure(
        &raw,
        &signature,
        SECRET,
        NOW,
        &config(),
        &binding(),
        &history,
    )
    .unwrap() else {
        panic!("expected linked failure");
    };
    *evidence
}

fn current_invoice(status: &str) -> Value {
    json!({
        "id":"in_failed", "customer":"cus_renewal",
        "parent":{"type":"subscription_details","subscription_details":{"subscription":"sub_renewal"}},
        "status":status,"billing_reason":"subscription_cycle",
        "collection_method":"charge_automatically","currency":"gbp",
        "amount_paid": if status == "paid" { 299 } else { 0 },"amount_due":299,
        "amount_remaining": if status == "open" { 299 } else { 0 },
        "amount_overpaid":0,"amount_paid_off_stripe":0,"livemode":false,
        "metadata":{"sotto_allocation_reference":"alloc_renewal"}
    })
}

fn current_line() -> Value {
    json!({
        "id":"il_failed","invoice":"in_failed","quantity":1,"livemode":false,
        "parent":{"type":"subscription_item_details","subscription_item_details":{
            "subscription":"sub_renewal","subscription_item":"si_renewal","proration":false
        }},
        "pricing":{"type":"price_details","price_details":{"price":"price_month"}},
        "period":{"start":2000,"end":3000}
    })
}

fn current_subscription(cancel_at_period_end: bool) -> Value {
    json!({"id":"sub_renewal","customer":"cus_renewal","status":"active","livemode":false,
        "cancel_at_period_end":cancel_at_period_end,"cancel_at":null,"canceled_at":null,"ended_at":null})
}

async fn current_client(
    status: &str,
    invoice_second: Option<Value>,
) -> (
    StripeReadClient,
    sotto_server::cloud_provider_stripe_http::StripeReadSession,
    MockServer,
) {
    let invoice = current_invoice(status);
    let second = invoice_second.unwrap_or_else(|| invoice.clone());
    current_client_with_parts(
        invoice,
        second,
        current_line(),
        current_line(),
        current_subscription(false),
        current_subscription(false),
    )
    .await
}

async fn current_client_with_parts(
    invoice: Value,
    second_invoice: Value,
    first_line: Value,
    second_line: Value,
    first_subscription: Value,
    second_subscription: Value,
) -> (
    StripeReadClient,
    sotto_server::cloud_provider_stripe_http::StripeReadSession,
    MockServer,
) {
    let mut responses = HashMap::new();
    responses.insert(
        "/v1/account".into(),
        vec![json!({"id":"acct_test_renewal","livemode":false})],
    );
    responses.insert(
        "/v1/subscriptions/sub_renewal".into(),
        vec![first_subscription, second_subscription],
    );
    responses.insert(
        "/v1/invoices/in_failed".into(),
        vec![invoice, second_invoice],
    );
    responses.insert(
        "/v1/invoices/in_failed/lines".into(),
        vec![
            list(vec![first_line], false),
            list(vec![second_line], false),
        ],
    );
    responses.insert(
        "/v1/invoice_payments".into(),
        vec![list(
            vec![json!({
                "id":"inpay_current","invoice":"in_failed","status":"paid","amount_paid":299,
                "amount_requested":299,"currency":"gbp","livemode":false,
                "payment":{"type":"payment_intent","payment_intent":"pi_current"}
            })],
            false,
        )],
    );
    for path in ["/v1/refunds", "/v1/disputes", "/v1/credit_notes"] {
        responses.insert(path.into(), vec![list(Vec::new(), false)]);
    }
    let server = mock_server(responses).await;
    let client = StripeReadClient::for_test(
        "sk_test_renewal",
        &config(),
        server.origin.clone(),
        limits(),
    )
    .unwrap();
    let session = client.session();
    (client, session, server)
}

#[tokio::test]
async fn current_paid_invoice_supersedes_historical_failure_and_preserves_cancellation_facts() {
    let failure = linked_failure().await;
    let (client, mut session, _server) = current_client("paid", None).await;
    let result = client
        .personal_renewal_observation(&mut session, &binding(), &failure)
        .await
        .unwrap();
    let StripeRenewalObservationResult::Observed(observation) = result else {
        panic!("expected observation")
    };
    assert_eq!(observation.invoice_id(), "in_failed");
    assert_eq!(observation.event_id(), "evt_failure_1");
    assert_eq!(observation.provider_account_id(), "acct_test_renewal");
    assert_eq!(observation.allocation_reference(), "alloc_renewal");
    assert_eq!(observation.period_start(), 2000);
    assert_eq!(observation.period_end(), 3000);
    assert!(matches!(
        observation.state(),
        StripeRenewalCurrentState::Paid { .. }
    ));
    assert!(!observation.cancellation().cancel_at_period_end());
}

#[tokio::test]
async fn paid_observation_reuses_the_validated_line_and_rejects_nonzero_remaining() {
    let failure = linked_failure().await;
    let mut changed_line = current_line();
    changed_line["period"]["start"] = json!(4000);
    changed_line["period"]["end"] = json!(5000);
    let mut nonzero_remaining = current_invoice("paid");
    nonzero_remaining["amount_remaining"] = json!(1);
    let (client, mut session, server) = current_client_with_parts(
        nonzero_remaining,
        current_invoice("paid"),
        current_line(),
        changed_line,
        current_subscription(false),
        current_subscription(false),
    )
    .await;
    let result = client
        .personal_renewal_observation(&mut session, &binding(), &failure)
        .await
        .unwrap();
    assert!(matches!(
        result,
        StripeRenewalObservationResult::NeedsEvidence(
            StripeRenewalNeedsEvidence::UnsupportedSettlement
        )
    ));
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request.contains("GET /v1/invoices/in_failed/lines"))
            .count(),
        1
    );
}

#[tokio::test]
async fn paid_observation_rejects_missing_null_and_negative_remaining() {
    let failure = linked_failure().await;
    for remaining in [None, Some(json!(null)), Some(json!(-1))] {
        let mut invoice = current_invoice("paid");
        match remaining {
            None => {
                invoice.as_object_mut().unwrap().remove("amount_remaining");
            }
            Some(value) => invoice["amount_remaining"] = value,
        }
        let (client, mut session, _server) = current_client_with_parts(
            invoice,
            current_invoice("paid"),
            current_line(),
            current_line(),
            current_subscription(false),
            current_subscription(false),
        )
        .await;
        let result = client
            .personal_renewal_observation(&mut session, &binding(), &failure)
            .await
            .unwrap();
        assert!(matches!(
            result,
            StripeRenewalObservationResult::NeedsEvidence(
                StripeRenewalNeedsEvidence::UnsupportedSettlement
            )
        ));
    }
}

#[tokio::test]
async fn paid_observation_keeps_the_first_line_interval() {
    let failure = linked_failure().await;
    let mut changed_line = current_line();
    changed_line["period"]["start"] = json!(4000);
    changed_line["period"]["end"] = json!(5000);
    let (client, mut session, server) = current_client_with_parts(
        current_invoice("paid"),
        current_invoice("paid"),
        current_line(),
        changed_line,
        current_subscription(false),
        current_subscription(false),
    )
    .await;
    let result = client
        .personal_renewal_observation(&mut session, &binding(), &failure)
        .await
        .unwrap();
    let StripeRenewalObservationResult::Observed(observation) = result else {
        panic!("expected observation");
    };
    assert_eq!(observation.period_start(), failure.renewal_period_start());
    assert_eq!(observation.period_end(), failure.renewal_period_end());
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request.contains("GET /v1/invoices/in_failed/lines"))
            .count(),
        1
    );
}

#[tokio::test]
async fn current_invoice_requires_an_automatic_subscription_cycle() {
    let failure = linked_failure().await;
    for (field, value) in [
        ("billing_reason", json!("manual")),
        ("collection_method", json!("send_invoice")),
    ] {
        let mut invoice = current_invoice("paid");
        invoice[field] = value.clone();
        let (client, mut session, _server) = current_client_with_parts(
            invoice,
            current_invoice("paid"),
            current_line(),
            current_line(),
            current_subscription(false),
            current_subscription(false),
        )
        .await;
        let result = client
            .personal_renewal_observation(&mut session, &binding(), &failure)
            .await
            .unwrap();
        assert!(matches!(
            result,
            StripeRenewalObservationResult::NeedsEvidence(
                StripeRenewalNeedsEvidence::UnsupportedInvoiceField {
                    field: actual,
                    value: ref actual_value,
                }
            ) if actual == field && actual_value == value.as_str().unwrap()
        ));
    }
}

#[tokio::test]
async fn current_invoice_requires_named_billing_fields() {
    let failure = linked_failure().await;
    for field in ["billing_reason", "collection_method"] {
        for (remove, replacement) in [
            (true, None),
            (false, Some(json!(null))),
            (false, Some(json!(42))),
        ] {
            let mut invoice = current_invoice("paid");
            if remove {
                invoice.as_object_mut().unwrap().remove(field);
            } else {
                invoice[field] = replacement.unwrap();
            }
            let (client, mut session, _server) = current_client_with_parts(
                invoice,
                current_invoice("paid"),
                current_line(),
                current_line(),
                current_subscription(false),
                current_subscription(false),
            )
            .await;
            assert!(matches!(
                client
                    .personal_renewal_observation(&mut session, &binding(), &failure)
                    .await,
                Err(StripeReadError::MalformedResponse(actual)) if actual == format!("invoice.{field}")
            ));
        }
    }
}

#[tokio::test]
async fn current_open_invoice_is_unresolved_and_does_not_become_recovery() {
    let failure = linked_failure().await;
    let (client, mut session, _server) = current_client("open", None).await;
    let result = client
        .personal_renewal_observation(&mut session, &binding(), &failure)
        .await
        .unwrap();
    let StripeRenewalObservationResult::Observed(observation) = result else {
        panic!("expected observation")
    };
    assert!(matches!(
        observation.state(),
        StripeRenewalCurrentState::Open
    ));
}

#[tokio::test]
async fn closed_invoice_is_observed_without_shortening_a_paid_term() {
    let failure = linked_failure().await;
    let (client, mut session, _server) = current_client("void", None).await;
    let result = client
        .personal_renewal_observation(&mut session, &binding(), &failure)
        .await
        .unwrap();
    let StripeRenewalObservationResult::Observed(observation) = result else {
        panic!("expected observation");
    };
    assert!(matches!(
        observation.state(),
        StripeRenewalCurrentState::ClosedUnpaid { status } if status == "void"
    ));
}

#[tokio::test]
async fn invoice_change_between_reads_returns_no_partial_observation() {
    let failure = linked_failure().await;
    let mut changed = current_invoice("paid");
    changed["metadata"]["sotto_allocation_reference"] = json!("other");
    let (client, mut session, _server) = current_client("paid", Some(changed)).await;
    let result = client
        .personal_renewal_observation(&mut session, &binding(), &failure)
        .await
        .unwrap();
    assert!(matches!(
        result,
        StripeRenewalObservationResult::NeedsEvidence(
            sotto_server::cloud_provider_stripe_http::StripeRenewalNeedsEvidence::ChangedDuringRead {
                resource: "invoice",
                ref fields,
            }
        ) if fields == &["allocation_reference"]
    ));
}

#[tokio::test]
async fn billing_and_cancellation_changes_name_the_provider_fields() {
    let failure = linked_failure().await;
    let mut changed_invoice = current_invoice("paid");
    changed_invoice["billing_reason"] = json!("subscription_update");
    let mut changed_subscription = current_subscription(false);
    changed_subscription
        .as_object_mut()
        .unwrap()
        .remove("canceled_at");
    let (client, mut session, _server) = current_client_with_parts(
        current_invoice("paid"),
        changed_invoice,
        current_line(),
        current_line(),
        current_subscription(false),
        changed_subscription,
    )
    .await;
    let result = client
        .personal_renewal_observation(&mut session, &binding(), &failure)
        .await
        .unwrap();
    assert!(matches!(
        result,
        StripeRenewalObservationResult::NeedsEvidence(
            StripeRenewalNeedsEvidence::ChangedDuringRead {
                resource: "invoice",
                ref fields,
            }
        ) if fields == &["billing_reason"]
    ));

    let (client, mut session, _server) = current_client_with_parts(
        current_invoice("paid"),
        current_invoice("paid"),
        current_line(),
        current_line(),
        current_subscription(false),
        {
            let mut subscription = current_subscription(false);
            subscription.as_object_mut().unwrap().remove("canceled_at");
            subscription
        },
    )
    .await;
    let result = client
        .personal_renewal_observation(&mut session, &binding(), &failure)
        .await
        .unwrap();
    assert!(matches!(
        result,
        StripeRenewalObservationResult::NeedsEvidence(
            StripeRenewalNeedsEvidence::ChangedDuringRead {
                resource: "subscription",
                ref fields,
            }
        )
        if fields == &["canceled_at"]
    ));
}

#[tokio::test]
async fn cancellation_fields_require_presence_and_valid_values() {
    let failure = linked_failure().await;
    let cases = [
        ("cancel_at", json!(null), true),
        ("cancel_at_period_end", json!(0), false),
        ("canceled_at", json!(-1), false),
    ];
    for (field, value, remove) in cases {
        let mut subscription = current_subscription(false);
        if remove {
            subscription.as_object_mut().unwrap().remove(field);
        } else {
            subscription[field] = value;
        }
        let (client, mut session, _server) = current_client_with_parts(
            current_invoice("paid"),
            current_invoice("paid"),
            current_line(),
            current_line(),
            subscription,
            current_subscription(false),
        )
        .await;
        assert!(matches!(
            client
                .personal_renewal_observation(&mut session, &binding(), &failure)
                .await,
            Err(StripeReadError::MalformedResponse(actual)) if actual == format!("subscription.{field}")
        ));
    }
}

#[tokio::test]
async fn collection_method_change_is_reported_by_name() {
    let failure = linked_failure().await;
    let mut changed_invoice = current_invoice("paid");
    changed_invoice["collection_method"] = json!("send_invoice");
    let (client, mut session, _server) = current_client_with_parts(
        current_invoice("paid"),
        changed_invoice,
        current_line(),
        current_line(),
        current_subscription(false),
        current_subscription(false),
    )
    .await;
    let result = client
        .personal_renewal_observation(&mut session, &binding(), &failure)
        .await
        .unwrap();
    assert!(matches!(
        result,
        StripeRenewalObservationResult::NeedsEvidence(
            StripeRenewalNeedsEvidence::ChangedDuringRead {
                resource: "invoice",
                ref fields,
            }
        ) if fields == &["collection_method"]
    ));
}

#[tokio::test]
async fn contradictory_legacy_line_ownership_is_rejected() {
    let failure = linked_failure().await;
    let mut line = current_line();
    line["subscription"] = json!("sub_other");
    let (client, mut session, _server) = current_client_with_parts(
        current_invoice("paid"),
        current_invoice("paid"),
        line,
        current_line(),
        current_subscription(false),
        current_subscription(false),
    )
    .await;
    assert!(matches!(
        client
            .personal_renewal_observation(&mut session, &binding(), &failure)
            .await,
        Err(StripeReadError::ContextMismatch)
    ));
}

#[tokio::test]
async fn matching_legacy_line_ownership_remains_supported() {
    let failure = linked_failure().await;
    let mut line = current_line();
    line["subscription"] = json!("sub_renewal");
    line["subscription_item"] = json!("si_renewal");
    let (client, mut session, _server) = current_client_with_parts(
        current_invoice("paid"),
        current_invoice("paid"),
        line,
        current_line(),
        current_subscription(false),
        current_subscription(false),
    )
    .await;
    assert!(matches!(
        client
            .personal_renewal_observation(&mut session, &binding(), &failure)
            .await,
        Ok(StripeRenewalObservationResult::Observed(_))
    ));
}

#[tokio::test]
async fn closed_invoice_rejects_remaining_balance_above_due() {
    let failure = linked_failure().await;
    let mut invoice = current_invoice("void");
    invoice["amount_remaining"] = json!(300);
    let (client, mut session, _server) = current_client_with_parts(
        invoice,
        current_invoice("void"),
        current_line(),
        current_line(),
        current_subscription(false),
        current_subscription(false),
    )
    .await;
    let result = client
        .personal_renewal_observation(&mut session, &binding(), &failure)
        .await
        .unwrap();
    assert!(matches!(
        result,
        StripeRenewalObservationResult::NeedsEvidence(
            StripeRenewalNeedsEvidence::UnsupportedSettlement
        )
    ));
}

#[tokio::test]
async fn foreign_binding_and_session_are_rejected_before_resource_reads() {
    let failure = linked_failure().await;
    let (client, mut session, server) = current_client("paid", None).await;
    let foreign = StripeAllocationBinding::new(
        "alloc_other",
        "cus_other",
        "sub_other",
        "si_other",
        PayerKind::Personal,
    )
    .unwrap();
    assert!(matches!(
        client
            .personal_renewal_observation(&mut session, &foreign, &failure)
            .await,
        Err(StripeReadError::ContextMismatch)
    ));
    assert!(server.requests().is_empty());

    let (other_client, other_session, other_server) = current_client("paid", None).await;
    let mut foreign_session = other_session;
    assert!(matches!(
        client
            .personal_renewal_observation(&mut foreign_session, &binding(), &failure)
            .await,
        Err(StripeReadError::SessionClientMismatch)
    ));
    assert!(server.requests().is_empty());
    assert!(other_server.requests().is_empty());
    drop(other_client);
}

#[tokio::test]
async fn open_to_paid_header_transition_returns_changed_fields_without_retrying() {
    let failure = linked_failure().await;
    let mut paid = current_invoice("paid");
    paid["amount_remaining"] = json!(0);
    let (client, mut session, server) = current_client_with_parts(
        current_invoice("open"),
        paid,
        current_line(),
        current_line(),
        current_subscription(false),
        current_subscription(false),
    )
    .await;
    let result = client
        .personal_renewal_observation(&mut session, &binding(), &failure)
        .await
        .unwrap();
    assert!(matches!(
        result,
        StripeRenewalObservationResult::NeedsEvidence(
            StripeRenewalNeedsEvidence::ChangedDuringRead {
                resource: "invoice",
                ref fields,
            }
        ) if fields == &["status", "amount_paid", "amount_remaining"]
    ));
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|request| request.as_str() == "GET /v1/invoices/in_failed")
            .count(),
        2
    );
}
