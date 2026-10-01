//! Pure composition of verified personal Stripe evidence.
//!
//! This module joins bounded invoice history to current renewal observations. It does not read
//! Stripe, write Postgres, assign recovery, or publish a coverage source. The result is a sealed,
//! deterministic candidate for the later provider adapter.

use std::cmp::Ordering;

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::cloud_provider::PayerKind;
use crate::cloud_provider_stripe::{StripeAllocationBinding, StripeCoverageConfig};
use crate::cloud_provider_stripe_corrections::{
    StripePersonalInvoiceCorrectionEvidence, StripeRetainedPaidTerm,
};
use crate::cloud_provider_stripe_http::{
    StripeNonPaidInvoice, StripePersonalInvoiceHistory, StripePersonalInvoiceHistoryEntry,
    StripeRenewalCancellationFacts, StripeRenewalCurrentState, StripeRenewalObservation,
};
use crate::cloud_provider_stripe_renewals::StripeRenewalFailureEvidence;

const SEMANTIC_DOMAIN: &[u8] = b"sotto-stripe-personal-coverage-v1\0";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StripeCoverageCompositionResult {
    Candidate(StripePersonalCoverageCandidate),
    NeedsEvidence(StripeCoverageNeedsEvidence),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StripeCoverageNeedsEvidence {
    MissingPredecessor {
        renewal_id: String,
        invoice_id: String,
    },
    AmbiguousPredecessor {
        renewal_id: String,
        invoice_id: String,
    },
    MissingCurrentInvoice {
        invoice_id: String,
    },
    ConflictingInvoice {
        invoice_id: String,
    },
    ConflictingRenewal {
        renewal_id: String,
    },
    ContradictoryState {
        invoice_id: String,
    },
    UnresolvedCorrection {
        invoice_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StripeCoverageCompositionError {
    #[error("personal Stripe coverage context does not match {field}")]
    ContextMismatch { field: &'static str },
    #[error("personal Stripe coverage input is invalid: {0}")]
    InvalidInput(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StripePersonalCoverageCandidate {
    account_id: String,
    environment: crate::cloud_provider::ProviderEnvironment,
    allocation_reference: String,
    customer_id: String,
    subscription_id: String,
    provider_item_id: String,
    paid_terms: Vec<StripeRetainedPaidTerm>,
    non_paid_invoices: Vec<StripeNonPaidInvoice>,
    renewals: Vec<StripeCoverageRenewal>,
    semantic_reference: String,
}

impl StripePersonalCoverageCandidate {
    pub fn account_id(&self) -> &str {
        &self.account_id
    }
    pub const fn environment(&self) -> crate::cloud_provider::ProviderEnvironment {
        self.environment
    }
    pub fn allocation_reference(&self) -> &str {
        &self.allocation_reference
    }
    pub fn customer_id(&self) -> &str {
        &self.customer_id
    }
    pub fn subscription_id(&self) -> &str {
        &self.subscription_id
    }
    pub fn provider_item_id(&self) -> &str {
        &self.provider_item_id
    }
    pub fn paid_terms(&self) -> &[StripeRetainedPaidTerm] {
        &self.paid_terms
    }
    pub fn non_paid_invoices(&self) -> &[StripeNonPaidInvoice] {
        &self.non_paid_invoices
    }
    pub fn renewals(&self) -> &[StripeCoverageRenewal] {
        &self.renewals
    }
    pub fn semantic_reference(&self) -> &str {
        &self.semantic_reference
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StripeCoverageRenewal {
    renewal_id: String,
    invoice_id: String,
    period_start: i64,
    period_end: i64,
    state: StripeCoverageRenewalState,
    cancellation: StripeRenewalCancellationFacts,
    event_ids: Vec<String>,
}

impl StripeCoverageRenewal {
    pub fn renewal_id(&self) -> &str {
        &self.renewal_id
    }
    pub fn invoice_id(&self) -> &str {
        &self.invoice_id
    }
    pub const fn period_start(&self) -> i64 {
        self.period_start
    }
    pub const fn period_end(&self) -> i64 {
        self.period_end
    }
    pub fn state(&self) -> &StripeCoverageRenewalState {
        &self.state
    }
    pub fn cancellation(&self) -> &StripeRenewalCancellationFacts {
        &self.cancellation
    }
    pub fn event_ids(&self) -> &[String] {
        &self.event_ids
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StripeCoverageRenewalState {
    Paid,
    Open,
    ClosedUnpaid { status: String },
}

/// Compose sealed history and current observations into one provisional candidate.
///
/// The operation is all-or-nothing. A candidate is never returned alongside unresolved evidence,
/// and no result from this function is an authoritative coverage interval or recovery decision.
pub fn compose_personal_coverage(
    config: &StripeCoverageConfig,
    binding: &StripeAllocationBinding,
    history: &StripePersonalInvoiceHistory,
    renewals: &[(StripeRenewalFailureEvidence, StripeRenewalObservation)],
) -> Result<StripeCoverageCompositionResult, StripeCoverageCompositionError> {
    config
        .validate()
        .map_err(|_| StripeCoverageCompositionError::InvalidInput("Stripe configuration"))?;
    if binding.payer_kind() != PayerKind::Personal {
        return Err(StripeCoverageCompositionError::ContextMismatch {
            field: "payer kind",
        });
    }
    check_history_context(config, binding, history)?;

    let mut paid_terms = Vec::new();
    let mut non_paid = Vec::new();
    for entry in history.entries() {
        match entry {
            StripePersonalInvoiceHistoryEntry::Paid(term) => {
                check_term_context(binding, term)?;
                if let Some(existing) =
                    paid_terms
                        .iter()
                        .find(|existing: &&StripeRetainedPaidTerm| {
                            existing.invoice_id() == term.invoice_id()
                        })
                {
                    if *existing != *term {
                        return Ok(needs(StripeCoverageNeedsEvidence::ConflictingInvoice {
                            invoice_id: term.invoice_id().to_owned(),
                        }));
                    }
                } else {
                    paid_terms.push(term.clone());
                }
            }
            StripePersonalInvoiceHistoryEntry::NonPaid(invoice) => {
                if non_paid.iter().any(|existing: &StripeNonPaidInvoice| {
                    existing.invoice_id() == invoice.invoice_id() && existing != invoice
                }) {
                    return Ok(needs(StripeCoverageNeedsEvidence::ConflictingInvoice {
                        invoice_id: invoice.invoice_id().to_owned(),
                    }));
                }
                if !non_paid
                    .iter()
                    .any(|existing: &StripeNonPaidInvoice| existing == invoice)
                {
                    non_paid.push(invoice.clone());
                }
            }
        }
    }

    let mut composed_renewals = Vec::new();
    for (failure, observation) in renewals {
        check_failure_context(config, binding, failure)?;
        check_observation_context(config, binding, observation)?;
        if failure.renewal_id() != observation.renewal_id()
            || failure.event_id() != observation.event_id()
            || failure.invoice_id() != observation.invoice_id()
            || failure.renewal_period_start() != observation.period_start()
            || failure.renewal_period_end() != observation.period_end()
        {
            return Err(StripeCoverageCompositionError::ContextMismatch {
                field: "renewal observation",
            });
        }
        let predecessor_matches = paid_terms
            .iter()
            .filter(|term| {
                term.invoice_id() == failure.predecessor_invoice_id()
                    && term.evidence_reference() == failure.predecessor_evidence_reference()
                    && term.period_start() == failure.predecessor_period_start()
                    && term.period_end() == failure.predecessor_period_end()
                    && term.period_end() == failure.renewal_period_start()
            })
            .count();
        if predecessor_matches == 0 {
            return Ok(needs(StripeCoverageNeedsEvidence::MissingPredecessor {
                renewal_id: failure.renewal_id().to_owned(),
                invoice_id: failure.predecessor_invoice_id().to_owned(),
            }));
        }
        if predecessor_matches > 1 {
            return Ok(needs(StripeCoverageNeedsEvidence::AmbiguousPredecessor {
                renewal_id: failure.renewal_id().to_owned(),
                invoice_id: failure.predecessor_invoice_id().to_owned(),
            }));
        }
        let Some(history_entry) = history
            .entries()
            .iter()
            .find(|entry| history_entry_invoice_id(entry) == failure.invoice_id())
        else {
            return Ok(needs(StripeCoverageNeedsEvidence::MissingCurrentInvoice {
                invoice_id: failure.invoice_id().to_owned(),
            }));
        };

        let state = match observation.state() {
            StripeRenewalCurrentState::Paid { evidence, term } => {
                check_term_context(binding, term)?;
                if term.invoice_id() != observation.invoice_id()
                    || term.period_start() != observation.period_start()
                    || term.period_end() != observation.period_end()
                {
                    return Ok(needs(StripeCoverageNeedsEvidence::ContradictoryState {
                        invoice_id: observation.invoice_id().to_owned(),
                    }));
                }
                let associated = match evidence.as_ref() {
                    StripePersonalInvoiceCorrectionEvidence::Associated(associated) => associated,
                    StripePersonalInvoiceCorrectionEvidence::Unresolved(_) => {
                        return Ok(needs(StripeCoverageNeedsEvidence::UnresolvedCorrection {
                            invoice_id: observation.invoice_id().to_owned(),
                        }));
                    }
                };
                let correction_observation = associated.observation();
                if correction_observation.invoice_id() != term.invoice_id()
                    || correction_observation.allocation_reference() != term.allocation_reference()
                    || correction_observation.customer_id() != term.customer_id()
                    || correction_observation.subscription_id() != term.subscription_id()
                    || correction_observation.provider_item_id() != term.provider_item_id()
                    || correction_observation.period_start() != term.period_start()
                    || correction_observation.period_end() != term.period_end()
                {
                    return Ok(needs(StripeCoverageNeedsEvidence::ContradictoryState {
                        invoice_id: observation.invoice_id().to_owned(),
                    }));
                }
                if let StripePersonalInvoiceHistoryEntry::Paid(existing) = history_entry {
                    if *existing != **term {
                        return Ok(needs(StripeCoverageNeedsEvidence::ConflictingInvoice {
                            invoice_id: observation.invoice_id().to_owned(),
                        }));
                    }
                }
                if let Some(position) = non_paid
                    .iter()
                    .position(|invoice| invoice.invoice_id() == observation.invoice_id())
                {
                    non_paid.remove(position);
                }
                if !paid_terms.iter().any(|existing| *existing == **term) {
                    paid_terms.push((**term).clone());
                }
                Some(StripeCoverageRenewalState::Paid)
            }
            StripeRenewalCurrentState::Open => {
                if !matches!(history_entry, StripePersonalInvoiceHistoryEntry::NonPaid(_)) {
                    return Ok(needs(StripeCoverageNeedsEvidence::ContradictoryState {
                        invoice_id: observation.invoice_id().to_owned(),
                    }));
                }
                Some(StripeCoverageRenewalState::Open)
            }
            StripeRenewalCurrentState::ClosedUnpaid { status } => {
                if !matches!(history_entry, StripePersonalInvoiceHistoryEntry::NonPaid(_)) {
                    return Ok(needs(StripeCoverageNeedsEvidence::ContradictoryState {
                        invoice_id: observation.invoice_id().to_owned(),
                    }));
                }
                Some(StripeCoverageRenewalState::ClosedUnpaid {
                    status: status.clone(),
                })
            }
        };
        let Some(state) = state else { continue };
        let next = StripeCoverageRenewal {
            renewal_id: observation.renewal_id().to_owned(),
            invoice_id: observation.invoice_id().to_owned(),
            period_start: observation.period_start(),
            period_end: observation.period_end(),
            state,
            cancellation: observation.cancellation().clone(),
            event_ids: vec![observation.event_id().to_owned()],
        };
        if let Some(existing) = composed_renewals
            .iter_mut()
            .find(|existing: &&mut StripeCoverageRenewal| existing.renewal_id == next.renewal_id)
        {
            if existing.invoice_id != next.invoice_id
                || existing.period_start != next.period_start
                || existing.period_end != next.period_end
                || existing.state != next.state
                || existing.cancellation != next.cancellation
            {
                return Ok(needs(StripeCoverageNeedsEvidence::ConflictingRenewal {
                    renewal_id: next.renewal_id,
                }));
            }
            existing.event_ids.extend(next.event_ids);
            existing.event_ids.sort();
            existing.event_ids.dedup();
        } else if composed_renewals
            .iter()
            .any(|existing| existing.invoice_id == next.invoice_id)
        {
            return Ok(needs(StripeCoverageNeedsEvidence::ConflictingInvoice {
                invoice_id: next.invoice_id,
            }));
        } else {
            composed_renewals.push(next);
        }
    }

    paid_terms.sort_by(term_order);
    non_paid.sort_by(|left, right| left.invoice_id().cmp(right.invoice_id()));
    composed_renewals.sort_by(|left, right| {
        left.period_start
            .cmp(&right.period_start)
            .then(left.period_end.cmp(&right.period_end))
            .then(left.invoice_id.cmp(&right.invoice_id))
            .then(left.renewal_id.cmp(&right.renewal_id))
    });
    let semantic_reference =
        semantic_reference(config, binding, &paid_terms, &non_paid, &composed_renewals);
    Ok(StripeCoverageCompositionResult::Candidate(
        StripePersonalCoverageCandidate {
            account_id: config.account_id.clone(),
            environment: config.environment,
            allocation_reference: binding.allocation_reference().to_owned(),
            customer_id: binding.customer_id().to_owned(),
            subscription_id: binding.subscription_id().to_owned(),
            provider_item_id: binding.provider_item_id().to_owned(),
            paid_terms,
            non_paid_invoices: non_paid,
            renewals: composed_renewals,
            semantic_reference,
        },
    ))
}

fn needs(reason: StripeCoverageNeedsEvidence) -> StripeCoverageCompositionResult {
    StripeCoverageCompositionResult::NeedsEvidence(reason)
}

fn check_history_context(
    config: &StripeCoverageConfig,
    binding: &StripeAllocationBinding,
    history: &StripePersonalInvoiceHistory,
) -> Result<(), StripeCoverageCompositionError> {
    if history.account_id() != config.account_id {
        return Err(StripeCoverageCompositionError::ContextMismatch { field: "account" });
    }
    if history.environment() != config.environment {
        return Err(StripeCoverageCompositionError::ContextMismatch {
            field: "environment",
        });
    }
    if history.subscription_id() != binding.subscription_id() {
        return Err(StripeCoverageCompositionError::ContextMismatch {
            field: "subscription",
        });
    }
    if history.customer_id() != binding.customer_id() {
        return Err(StripeCoverageCompositionError::ContextMismatch { field: "customer" });
    }
    Ok(())
}

fn check_term_context(
    binding: &StripeAllocationBinding,
    term: &StripeRetainedPaidTerm,
) -> Result<(), StripeCoverageCompositionError> {
    if term.allocation_reference() != binding.allocation_reference() {
        return Err(StripeCoverageCompositionError::ContextMismatch {
            field: "allocation",
        });
    }
    if term.customer_id() != binding.customer_id() {
        return Err(StripeCoverageCompositionError::ContextMismatch { field: "customer" });
    }
    if term.subscription_id() != binding.subscription_id() {
        return Err(StripeCoverageCompositionError::ContextMismatch {
            field: "subscription",
        });
    }
    if term.provider_item_id() != binding.provider_item_id() {
        return Err(StripeCoverageCompositionError::ContextMismatch {
            field: "provider item",
        });
    }
    Ok(())
}

fn check_failure_context(
    config: &StripeCoverageConfig,
    binding: &StripeAllocationBinding,
    failure: &StripeRenewalFailureEvidence,
) -> Result<(), StripeCoverageCompositionError> {
    if failure.provider_account_id() != config.account_id {
        return Err(StripeCoverageCompositionError::ContextMismatch { field: "account" });
    }
    if failure.environment() != config.environment {
        return Err(StripeCoverageCompositionError::ContextMismatch {
            field: "environment",
        });
    }
    for (actual, expected, field) in [
        (
            failure.allocation_reference(),
            binding.allocation_reference(),
            "allocation",
        ),
        (failure.customer_id(), binding.customer_id(), "customer"),
        (
            failure.subscription_id(),
            binding.subscription_id(),
            "subscription",
        ),
        (
            failure.provider_item_id(),
            binding.provider_item_id(),
            "provider item",
        ),
    ] {
        if actual != expected {
            return Err(StripeCoverageCompositionError::ContextMismatch { field });
        }
    }
    Ok(())
}

fn check_observation_context(
    config: &StripeCoverageConfig,
    binding: &StripeAllocationBinding,
    observation: &StripeRenewalObservation,
) -> Result<(), StripeCoverageCompositionError> {
    if observation.provider_account_id() != config.account_id {
        return Err(StripeCoverageCompositionError::ContextMismatch { field: "account" });
    }
    if observation.environment() != config.environment {
        return Err(StripeCoverageCompositionError::ContextMismatch {
            field: "environment",
        });
    }
    for (actual, expected, field) in [
        (
            observation.allocation_reference(),
            binding.allocation_reference(),
            "allocation",
        ),
        (observation.customer_id(), binding.customer_id(), "customer"),
        (
            observation.subscription_id(),
            binding.subscription_id(),
            "subscription",
        ),
        (
            observation.provider_item_id(),
            binding.provider_item_id(),
            "provider item",
        ),
    ] {
        if actual != expected {
            return Err(StripeCoverageCompositionError::ContextMismatch { field });
        }
    }
    Ok(())
}

fn history_entry_invoice_id(entry: &StripePersonalInvoiceHistoryEntry) -> &str {
    match entry {
        StripePersonalInvoiceHistoryEntry::Paid(term) => term.invoice_id(),
        StripePersonalInvoiceHistoryEntry::NonPaid(invoice) => invoice.invoice_id(),
    }
}

fn term_order(left: &StripeRetainedPaidTerm, right: &StripeRetainedPaidTerm) -> Ordering {
    left.period_start()
        .cmp(&right.period_start())
        .then(left.period_end().cmp(&right.period_end()))
        .then(left.invoice_id().cmp(right.invoice_id()))
}

fn semantic_reference(
    config: &StripeCoverageConfig,
    binding: &StripeAllocationBinding,
    paid_terms: &[StripeRetainedPaidTerm],
    non_paid: &[StripeNonPaidInvoice],
    renewals: &[StripeCoverageRenewal],
) -> String {
    let mut bytes = SEMANTIC_DOMAIN.to_vec();
    field(&mut bytes, &config.account_id);
    field(&mut bytes, config.environment.as_str());
    field(&mut bytes, binding.allocation_reference());
    field(&mut bytes, binding.customer_id());
    field(&mut bytes, binding.subscription_id());
    field(&mut bytes, binding.provider_item_id());
    for term in paid_terms {
        field(&mut bytes, "paid");
        field(&mut bytes, term.invoice_id());
        field(&mut bytes, term.evidence_reference());
        field_i64(&mut bytes, term.period_start());
        field_i64(&mut bytes, term.period_end());
        field(&mut bytes, &term.interval().to_string());
    }
    for invoice in non_paid {
        field(&mut bytes, "non-paid");
        field(&mut bytes, invoice.invoice_id());
        field(&mut bytes, invoice.status());
    }
    for renewal in renewals {
        field(&mut bytes, "renewal");
        field(&mut bytes, renewal.renewal_id());
        field(&mut bytes, renewal.invoice_id());
        field_i64(&mut bytes, renewal.period_start());
        field_i64(&mut bytes, renewal.period_end());
        match &renewal.state {
            StripeCoverageRenewalState::Paid => field(&mut bytes, "paid"),
            StripeCoverageRenewalState::Open => field(&mut bytes, "open"),
            StripeCoverageRenewalState::ClosedUnpaid { status } => {
                field(&mut bytes, "closed-unpaid");
                field(&mut bytes, status);
            }
        }
        field_bool(&mut bytes, renewal.cancellation.cancel_at_period_end());
        field(&mut bytes, renewal.cancellation.status().unwrap_or("null"));
        field_opt_i64(&mut bytes, renewal.cancellation.cancel_at());
        field_opt_i64(&mut bytes, renewal.cancellation.canceled_at());
        field_opt_i64(&mut bytes, renewal.cancellation.ended_at());
    }
    let digest = Sha256::digest(bytes);
    format!("stripe-personal-coverage-v1:{digest:x}")
}

fn field(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(&(value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
}

fn field_i64(bytes: &mut Vec<u8>, value: i64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn field_opt_i64(bytes: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => {
            bytes.push(1);
            field_i64(bytes, value);
        }
        None => bytes.push(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud_provider::{PayerKind, ProviderEnvironment};
    use crate::cloud_provider_stripe::{StripeInterval, StripePersonalInvoiceObservation};
    use crate::cloud_provider_stripe_corrections::{
        StripeAssociatedCorrectionEvidence, StripePersonalInvoiceCorrectionEvidence,
    };

    fn config() -> StripeCoverageConfig {
        StripeCoverageConfig::new(
            "acct_test",
            ProviderEnvironment::Test,
            "price_month",
            "price_year",
        )
        .unwrap()
    }

    fn binding() -> StripeAllocationBinding {
        StripeAllocationBinding::new("alloc", "cus", "sub", "si", PayerKind::Personal).unwrap()
    }

    fn term(invoice_id: &str, start: i64, end: i64, evidence: &str) -> StripeRetainedPaidTerm {
        StripeRetainedPaidTerm::test_new(StripePersonalInvoiceObservation::test_new(
            invoice_id,
            "cus",
            "sub",
            "si",
            "alloc",
            StripeInterval::Month,
            start,
            end,
            evidence,
        ))
    }

    fn history(entries: Vec<StripePersonalInvoiceHistoryEntry>) -> StripePersonalInvoiceHistory {
        StripePersonalInvoiceHistory::test_new(
            "acct_test",
            ProviderEnvironment::Test,
            "sub",
            "cus",
            entries,
        )
    }

    fn failure(event_id: &str, renewal_id: &str) -> StripeRenewalFailureEvidence {
        StripeRenewalFailureEvidence::test_new(
            renewal_id,
            event_id,
            "in_failed",
            "in_paid",
            "ev_paid",
            "acct_test",
            ProviderEnvironment::Test,
            "alloc",
            "cus",
            "sub",
            "si",
            1000,
            2000,
            2000,
            3000,
            StripeInterval::Month,
        )
    }

    fn observation(
        failure: &StripeRenewalFailureEvidence,
        state: StripeRenewalCurrentState,
    ) -> StripeRenewalObservation {
        observation_with_cancel(failure, state, None)
    }

    fn observation_with_cancel(
        failure: &StripeRenewalFailureEvidence,
        state: StripeRenewalCurrentState,
        cancel_at: Option<i64>,
    ) -> StripeRenewalObservation {
        StripeRenewalObservation::test_new(
            failure.renewal_id(),
            failure.invoice_id(),
            failure.event_id(),
            "acct_test",
            ProviderEnvironment::Test,
            "alloc",
            "cus",
            "sub",
            "si",
            2000,
            3000,
            state,
            StripeRenewalCancellationFacts::test_new(
                Some("active"),
                cancel_at.is_some(),
                cancel_at,
                None,
                None,
            ),
        )
    }

    fn paid_observation(failure: &StripeRenewalFailureEvidence) -> StripeRenewalObservation {
        let inner = StripePersonalInvoiceObservation::test_new(
            "in_failed",
            "cus",
            "sub",
            "si",
            "alloc",
            StripeInterval::Month,
            2000,
            3000,
            "ev_current",
        );
        let term = StripeRetainedPaidTerm::test_new(inner.clone());
        let evidence = StripePersonalInvoiceCorrectionEvidence::Associated(Box::new(
            StripeAssociatedCorrectionEvidence::test_new(inner),
        ));
        observation(
            failure,
            StripeRenewalCurrentState::Paid {
                evidence: Box::new(evidence),
                term: Box::new(term),
            },
        )
    }

    #[test]
    fn history_order_does_not_change_candidate_identity() {
        let first = history(vec![
            StripePersonalInvoiceHistoryEntry::Paid(term("in_two", 2000, 3000, "ev_two")),
            StripePersonalInvoiceHistoryEntry::Paid(term("in_one", 1000, 2000, "ev_one")),
        ]);
        let second = history(vec![
            StripePersonalInvoiceHistoryEntry::Paid(term("in_one", 1000, 2000, "ev_one")),
            StripePersonalInvoiceHistoryEntry::Paid(term("in_two", 2000, 3000, "ev_two")),
        ]);
        let first = compose_personal_coverage(&config(), &binding(), &first, &[]).unwrap();
        let second = compose_personal_coverage(&config(), &binding(), &second, &[]).unwrap();
        let (
            StripeCoverageCompositionResult::Candidate(first),
            StripeCoverageCompositionResult::Candidate(second),
        ) = (first, second)
        else {
            panic!("expected candidates");
        };
        assert_eq!(first.semantic_reference(), second.semantic_reference());
        assert_eq!(first.paid_terms()[0].invoice_id(), "in_one");
    }

    #[test]
    fn mismatched_history_context_is_rejected_before_candidate_creation() {
        let history = StripePersonalInvoiceHistory::test_new(
            "acct_other",
            ProviderEnvironment::Test,
            "sub",
            "cus",
            vec![],
        );
        assert_eq!(
            compose_personal_coverage(&config(), &binding(), &history, &[]),
            Err(StripeCoverageCompositionError::ContextMismatch { field: "account" })
        );
    }

    #[test]
    fn open_renewal_keeps_predecessor_and_does_not_assign_recovery() {
        let failure = failure("evt_one", "renewal_one");
        let history = history(vec![
            StripePersonalInvoiceHistoryEntry::Paid(term("in_paid", 1000, 2000, "ev_paid")),
            StripePersonalInvoiceHistoryEntry::NonPaid(StripeNonPaidInvoice::test_new(
                "in_failed",
                "open",
            )),
        ]);
        let observation = observation(&failure, StripeRenewalCurrentState::Open);
        let result =
            compose_personal_coverage(&config(), &binding(), &history, &[(failure, observation)])
                .unwrap();
        let StripeCoverageCompositionResult::Candidate(candidate) = result else {
            panic!("expected candidate");
        };
        assert_eq!(candidate.paid_terms().len(), 1);
        assert!(matches!(
            candidate.renewals()[0].state(),
            StripeCoverageRenewalState::Open
        ));
    }

    #[test]
    fn paid_transition_replaces_non_paid_entry_and_retry_keeps_identity() {
        let first_failure = failure("evt_one", "renewal_one");
        let second_failure = failure("evt_two", "renewal_one");
        let history = history(vec![
            StripePersonalInvoiceHistoryEntry::Paid(term("in_paid", 1000, 2000, "ev_paid")),
            StripePersonalInvoiceHistoryEntry::NonPaid(StripeNonPaidInvoice::test_new(
                "in_failed",
                "open",
            )),
        ]);
        let first = paid_observation(&first_failure);
        let second = paid_observation(&second_failure);
        let result = compose_personal_coverage(
            &config(),
            &binding(),
            &history,
            &[(first_failure, first), (second_failure, second)],
        )
        .unwrap();
        let StripeCoverageCompositionResult::Candidate(candidate) = result else {
            panic!("expected candidate");
        };
        assert_eq!(candidate.paid_terms().len(), 2);
        assert!(candidate.non_paid_invoices().is_empty());
        assert_eq!(candidate.renewals()[0].event_ids(), &["evt_one", "evt_two"]);
        assert!(matches!(
            candidate.renewals()[0].state(),
            StripeCoverageRenewalState::Paid
        ));
    }

    #[test]
    fn retry_event_ids_do_not_change_semantic_identity() {
        let history = history(vec![
            StripePersonalInvoiceHistoryEntry::Paid(term("in_paid", 1000, 2000, "ev_paid")),
            StripePersonalInvoiceHistoryEntry::NonPaid(StripeNonPaidInvoice::test_new(
                "in_failed",
                "open",
            )),
        ]);
        let first_failure = failure("evt_one", "renewal_one");
        let second_failure = failure("evt_two", "renewal_one");
        let first = compose_personal_coverage(
            &config(),
            &binding(),
            &history,
            &[(first_failure.clone(), paid_observation(&first_failure))],
        )
        .unwrap();
        let second = compose_personal_coverage(
            &config(),
            &binding(),
            &history,
            &[(second_failure.clone(), paid_observation(&second_failure))],
        )
        .unwrap();
        let (
            StripeCoverageCompositionResult::Candidate(first),
            StripeCoverageCompositionResult::Candidate(second),
        ) = (first, second)
        else {
            panic!("expected candidates");
        };
        assert_eq!(first.semantic_reference(), second.semantic_reference());
        assert_ne!(
            first.renewals()[0].event_ids(),
            second.renewals()[0].event_ids()
        );
    }

    #[test]
    fn cancellation_change_changes_semantic_identity() {
        let failure = failure("evt_one", "renewal_one");
        let history = history(vec![
            StripePersonalInvoiceHistoryEntry::Paid(term("in_paid", 1000, 2000, "ev_paid")),
            StripePersonalInvoiceHistoryEntry::NonPaid(StripeNonPaidInvoice::test_new(
                "in_failed",
                "open",
            )),
        ]);
        let active = compose_personal_coverage(
            &config(),
            &binding(),
            &history,
            &[(
                failure.clone(),
                observation_with_cancel(&failure, StripeRenewalCurrentState::Open, None),
            )],
        )
        .unwrap();
        let scheduled = compose_personal_coverage(
            &config(),
            &binding(),
            &history,
            &[(
                failure.clone(),
                observation_with_cancel(&failure, StripeRenewalCurrentState::Open, Some(3500)),
            )],
        )
        .unwrap();
        let (
            StripeCoverageCompositionResult::Candidate(active),
            StripeCoverageCompositionResult::Candidate(scheduled),
        ) = (active, scheduled)
        else {
            panic!("expected candidates");
        };
        assert_ne!(active.semantic_reference(), scheduled.semantic_reference());
    }

    #[test]
    fn unresolved_correction_cannot_become_a_paid_candidate() {
        let failure = failure("evt_one", "renewal_one");
        let history = history(vec![
            StripePersonalInvoiceHistoryEntry::Paid(term("in_paid", 1000, 2000, "ev_paid")),
            StripePersonalInvoiceHistoryEntry::NonPaid(StripeNonPaidInvoice::test_new(
                "in_failed",
                "open",
            )),
        ]);
        let inner = StripePersonalInvoiceObservation::test_new(
            "in_failed",
            "cus",
            "sub",
            "si",
            "alloc",
            StripeInterval::Month,
            2000,
            3000,
            "ev_current",
        );
        let term = StripeRetainedPaidTerm::test_new(inner.clone());
        let observation = observation(
            &failure,
            StripeRenewalCurrentState::Paid {
                evidence: Box::new(StripePersonalInvoiceCorrectionEvidence::Unresolved(
                    crate::cloud_provider_stripe_corrections::StripeUnresolvedCorrections::test_new(
                        "in_failed",
                    ),
                )),
                term: Box::new(term),
            },
        );
        assert!(matches!(
            compose_personal_coverage(&config(), &binding(), &history, &[(failure, observation)]),
            Ok(StripeCoverageCompositionResult::NeedsEvidence(
                StripeCoverageNeedsEvidence::UnresolvedCorrection { .. }
            ))
        ));
    }

    #[test]
    fn conflicting_retries_never_choose_by_input_order() {
        let first_failure = failure("evt_one", "renewal_one");
        let second_failure = failure("evt_two", "renewal_one");
        let history = history(vec![
            StripePersonalInvoiceHistoryEntry::Paid(term("in_paid", 1000, 2000, "ev_paid")),
            StripePersonalInvoiceHistoryEntry::NonPaid(StripeNonPaidInvoice::test_new(
                "in_failed",
                "open",
            )),
        ]);
        let first = observation(&first_failure, StripeRenewalCurrentState::Open);
        let second = observation(
            &second_failure,
            StripeRenewalCurrentState::ClosedUnpaid {
                status: "void".to_owned(),
            },
        );
        assert_eq!(
            compose_personal_coverage(
                &config(),
                &binding(),
                &history,
                &[(first_failure, first), (second_failure, second)],
            )
            .unwrap(),
            StripeCoverageCompositionResult::NeedsEvidence(
                StripeCoverageNeedsEvidence::ConflictingRenewal {
                    renewal_id: "renewal_one".to_owned(),
                }
            )
        );
    }

    #[test]
    fn missing_predecessor_exposes_no_partial_candidate() {
        let failure = failure("evt_one", "renewal_one");
        let history = history(vec![StripePersonalInvoiceHistoryEntry::NonPaid(
            StripeNonPaidInvoice::test_new("in_failed", "open"),
        )]);
        let observation = observation(&failure, StripeRenewalCurrentState::Open);
        assert!(matches!(
            compose_personal_coverage(&config(), &binding(), &history, &[(failure, observation)]),
            Ok(StripeCoverageCompositionResult::NeedsEvidence(
                StripeCoverageNeedsEvidence::MissingPredecessor { .. }
            ))
        ));
    }
}

fn field_bool(bytes: &mut Vec<u8>, value: bool) {
    bytes.push(u8::from(value));
}
