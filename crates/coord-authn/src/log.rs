//! The bounded log of admitted receipts (design Sections 9.3, 19.4):
//! what was admitted, under which rule, for how long. The raw token is
//! never recorded, only the receipt identity and the stable identity.

use std::collections::VecDeque;

use coord_types::identity::Digest32;
use coord_types::ids::{SessionId, TrustRuleId};
use serde::{Deserialize, Serialize};

use crate::receipt::Admitted;
use crate::verifier::VerifiedIdentity;

/// One admitted receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmittedRecord {
    /// Receipt identity.
    pub receipt_id: Digest32,
    /// Session created.
    pub session: SessionId,
    /// Issuer configuration name.
    pub issuer: String,
    /// Subject.
    pub subject: String,
    /// Rule used.
    pub rule: TrustRuleId,
    /// Admission time (unix seconds).
    pub admitted_at: u64,
    /// Conservative validity end.
    pub valid_until: u64,
}

/// A bounded ring of admitted receipts.
#[derive(Debug)]
pub struct AdmissionLog {
    max: usize,
    records: VecDeque<AdmittedRecord>,
}

impl AdmissionLog {
    /// A log keeping the last `max` records.
    pub const fn new(max: usize) -> Self {
        AdmissionLog {
            max,
            records: VecDeque::new(),
        }
    }

    /// Record an admission.
    pub fn record(&mut self, identity: &VerifiedIdentity, admitted: &Admitted, now: u64) {
        self.records.push_back(AdmittedRecord {
            receipt_id: admitted.receipt.receipt_id,
            session: admitted.session,
            issuer: identity.name.clone(),
            subject: identity.subject.clone(),
            rule: admitted.rule,
            admitted_at: now,
            valid_until: admitted.valid_until,
        });
        while self.records.len() > self.max {
            self.records.pop_front();
        }
    }

    /// The records, oldest first.
    pub fn records(&self) -> impl Iterator<Item = &AdmittedRecord> {
        self.records.iter()
    }
}
