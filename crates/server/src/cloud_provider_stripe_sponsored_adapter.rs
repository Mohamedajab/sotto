//! Convert a composed sponsored Stripe snapshot into one beneficiary collection.
//!
//! Sponsored invoices cover several named beneficiaries at once, while the reconciliation
//! boundary publishes one beneficiary's registered source set at a time.  This module is the
//! narrow, provider-specific seam between those shapes.  It consumes only the authenticated,
//! composed candidate and a durable collection ticket; it does not read Stripe or Postgres and
//! it does not start a refresh job.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use thiserror::Error;

use crate::cloud_coverage_reconciliation::{CollectionStatus, CollectionTicket, SourceObservation};
use crate::cloud_provider::{PayerKind, ProviderContext, VerifiedCollection};
use crate::cloud_provider_stripe::STRIPE_NAMESPACE;
use crate::cloud_provider_stripe_sponsored::SponsoredCoverageCandidate;

/// Build the provider-neutral collection for the beneficiary captured by a ticket.
///
/// The candidate can contain several beneficiaries.  Only observations whose source belongs to
/// the ticket beneficiary are returned, and the ticket's source set must match that beneficiary's
/// composed sources exactly.  This prevents a grouped invoice from publishing a sibling's facts
/// into the current projection or from silently publishing an incomplete source batch.
pub(crate) fn collection_for_ticket(
    context: &ProviderContext,
    ticket: &CollectionTicket,
    candidate: &SponsoredCoverageCandidate,
) -> Result<VerifiedCollection, SponsoredAdapterError> {
    validate_context(context, ticket, candidate)?;
    if ticket.status != CollectionStatus::Pending || ticket.completed_revision.is_some() {
        return Err(SponsoredAdapterError::InvalidTicket);
    }
    if ticket.source_bindings.is_empty() || ticket.beneficiary_id.trim().is_empty() {
        return Err(SponsoredAdapterError::InvalidTicket);
    }

    let expected_sources = ticket
        .source_bindings
        .iter()
        .filter(|binding| binding.beneficiary_id == ticket.beneficiary_id)
        .map(|binding| binding.source_id.as_str())
        .collect::<BTreeSet<_>>();
    if expected_sources.len() != ticket.source_bindings.len() {
        return Err(SponsoredAdapterError::InvalidTicket);
    }

    let mut beneficiary_sources = BTreeSet::<String>::new();
    for coverage in candidate.person_coverages() {
        if coverage.beneficiary_id == ticket.beneficiary_id {
            beneficiary_sources.extend(
                coverage
                    .paid_intervals
                    .into_iter()
                    .map(|interval| interval.source_id),
            );
        }
    }
    if beneficiary_sources
        != expected_sources
            .iter()
            .map(|source_id| (*source_id).to_owned())
            .collect::<BTreeSet<_>>()
    {
        return Err(SponsoredAdapterError::SourceSetMismatch);
    }

    let mut observations_by_source = BTreeMap::new();
    for observation in candidate.source_observations() {
        let source_id = match &observation {
            SourceObservation::Complete { source_id, .. }
            | SourceObservation::Unavailable { source_id, .. } => source_id,
        };
        if observations_by_source
            .insert(source_id.clone(), observation)
            .is_some()
        {
            return Err(SponsoredAdapterError::DuplicateObservation);
        }
    }

    let mut observations = Vec::with_capacity(expected_sources.len());
    for source_id in expected_sources {
        let observation = observations_by_source
            .remove(source_id)
            .ok_or(SponsoredAdapterError::MissingObservation)?;
        observations.push(observation);
    }

    Ok(VerifiedCollection {
        aggregate_evidence_reference: format!(
            "stripe-sponsored-collection-v1:{}:{}",
            candidate.semantic_reference(),
            ticket.beneficiary_id
        ),
        observations,
    })
}

fn validate_context(
    context: &ProviderContext,
    ticket: &CollectionTicket,
    candidate: &SponsoredCoverageCandidate,
) -> Result<(), SponsoredAdapterError> {
    if context.namespace != STRIPE_NAMESPACE
        || context.account_id != candidate.account_id()
        || context.environment != candidate.environment()
    {
        return Err(SponsoredAdapterError::ContextMismatch);
    }
    if candidate.payer_kind() != PayerKind::Sponsor {
        return Err(SponsoredAdapterError::UnsupportedPayer);
    }
    if ticket.beneficiary_id.trim().is_empty()
        || ticket.source_bindings.iter().any(|binding| {
            binding.provider_namespace != STRIPE_NAMESPACE
                || binding.beneficiary_id.trim().is_empty()
                || binding.source_id.trim().is_empty()
        })
    {
        return Err(SponsoredAdapterError::InvalidTicket);
    }
    Ok(())
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SponsoredAdapterError {
    #[error("sponsored candidate does not match the provider context")]
    ContextMismatch,
    #[error("sponsored collection ticket is invalid")]
    InvalidTicket,
    #[error("sponsored candidate is not payer-owned")]
    UnsupportedPayer,
    #[error("sponsored candidate does not cover the ticket source set")]
    SourceSetMismatch,
    #[error("sponsored candidate is missing a ticket source observation")]
    MissingObservation,
    #[error("sponsored candidate contains duplicate source observations")]
    DuplicateObservation,
}

