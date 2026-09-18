//! Clock health (design Section 9.4): validity is judged with an injected
//! reading and its uncertainty, conservatively on both ends. When the
//! clock is not healthy, validity cannot be established and admission is
//! denied. This is admission-time I/O policy, separate from consensus.

/// A clock reading with its health and uncertainty.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClockHealth {
    /// Unix seconds.
    pub now: u64,
    /// Bound on the reading's error, in seconds.
    pub uncertainty: u64,
    /// Whether the source is trusted right now (synchronized, within its
    /// configured uncertainty).
    pub healthy: bool,
}

/// Why a token's time claims were not accepted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimeError {
    /// The clock is not healthy: validity cannot be established.
    ClockUnhealthy,
    /// `exp` is not conservatively in the future.
    Expired,
    /// `nbf` is not conservatively in the past.
    NotYetValid,
    /// `iat` is conservatively in the future.
    IssuedInFuture,
    /// `iat` is older than the configured maximum age.
    TooOld,
}

impl ClockHealth {
    /// A healthy reading.
    pub const fn healthy(now: u64, uncertainty: u64) -> Self {
        ClockHealth {
            now,
            uncertainty,
            healthy: true,
        }
    }

    /// The latest instant the reading may denote.
    pub const fn latest(&self) -> u64 {
        self.now.saturating_add(self.uncertainty)
    }

    /// The earliest instant the reading may denote.
    pub const fn earliest(&self) -> u64 {
        self.now.saturating_sub(self.uncertainty)
    }

    /// Check `exp`, `nbf`, `iat` and a maximum age conservatively.
    pub fn check(
        &self,
        exp: u64,
        nbf: Option<u64>,
        iat: Option<u64>,
        max_age: Option<u64>,
    ) -> Result<(), TimeError> {
        if !self.healthy {
            return Err(TimeError::ClockUnhealthy);
        }
        if exp <= self.latest() {
            return Err(TimeError::Expired);
        }
        if let Some(nbf) = nbf
            && nbf > self.earliest()
        {
            return Err(TimeError::NotYetValid);
        }
        if let Some(iat) = iat {
            if iat > self.latest() {
                return Err(TimeError::IssuedInFuture);
            }
            if let Some(max) = max_age
                && self.earliest().saturating_sub(iat) > max
            {
                return Err(TimeError::TooOld);
            }
        }
        Ok(())
    }

    /// The conservative validity end of a token expiring at `exp`.
    pub const fn valid_until(&self, exp: u64) -> u64 {
        exp.saturating_sub(self.uncertainty)
    }
}
