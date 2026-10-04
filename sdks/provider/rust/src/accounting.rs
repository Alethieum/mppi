//! Pure report validation over provider-supplied payment state.
//!
//! These calculations do not grant execution, prove termination, allocate meter
//! sequences, or retain state. The provider runtime must apply them using its
//! consistent session view and immutable accepted run prices.

use alloy_primitives::U256;

use crate::amounts::{Rates, TokenCounts, priced_numerator};
use crate::errors::{ValidationCode, ValidationError};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunStatus {
    Completed,
    Failed,
    Cancelled,
    NotAdmitted,
}

impl RunStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
            Self::NotAdmitted => "NOT_ADMITTED",
        }
    }
}

/// Validated accounting for an admitted run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunAccounting {
    counts: TokenCounts,
    remaining_numerator: U256,
    final_status: Option<RunStatus>,
}

impl RunAccounting {
    pub fn new(
        counts: TokenCounts,
        remaining_numerator: U256,
        final_status: Option<RunStatus>,
    ) -> Result<Self, ValidationError> {
        validate_final_status(final_status)?;
        if final_status.is_some() && remaining_numerator != U256::ZERO {
            return Err(ValidationError::new(
                ValidationCode::InvalidValue,
                "final accounting has no remaining reservation",
            ));
        }
        Ok(Self {
            counts,
            remaining_numerator,
            final_status,
        })
    }

    pub const fn counts(self) -> TokenCounts {
        self.counts
    }

    pub const fn remaining_numerator(self) -> U256 {
        self.remaining_numerator
    }

    pub const fn final_status(self) -> Option<RunStatus> {
        self.final_status
    }
}

/// One immutable result conserving usage and remaining reservation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccountingTransition {
    state: RunAccounting,
    delta_numerator: U256,
    released_numerator: U256,
    duplicate: bool,
}

impl AccountingTransition {
    pub const fn state(self) -> RunAccounting {
        self.state
    }

    pub const fn delta_numerator(self) -> U256 {
        self.delta_numerator
    }

    pub const fn released_numerator(self) -> U256 {
        self.released_numerator
    }

    pub const fn duplicate(self) -> bool {
        self.duplicate
    }
}

fn validate_final_status(status: Option<RunStatus>) -> Result<(), ValidationError> {
    if status == Some(RunStatus::NotAdmitted) {
        return Err(ValidationError::new(
            ValidationCode::InvalidValue,
            "admitted run requires a definitive final status",
        ));
    }
    Ok(())
}

/// Validate cumulative reports and release unused coverage only on a final.
///
/// Exact repeated final reports are no-ops. Conflicting final reports,
/// decreasing counts/cost, and usage exceeding the reservation fail without
/// changing `previous`.
pub fn report_transition(
    previous: &RunAccounting,
    counts: TokenCounts,
    rates: Rates,
    final_status: Option<RunStatus>,
) -> Result<AccountingTransition, ValidationError> {
    validate_final_status(final_status)?;
    let old_numerator = priced_numerator(previous.counts, rates)?;
    let new_numerator = priced_numerator(counts, rates)?;
    if previous.final_status.is_some() {
        if counts == previous.counts && final_status == previous.final_status {
            return Ok(AccountingTransition {
                state: *previous,
                delta_numerator: U256::ZERO,
                released_numerator: U256::ZERO,
                duplicate: true,
            });
        }
        return Err(ValidationError::new(
            ValidationCode::InconsistentUsage,
            "report conflicts with definitive final accounting",
        ));
    }
    let old_counts = previous.counts;
    if counts.input_tokens() < old_counts.input_tokens()
        || counts.cache_tokens() < old_counts.cache_tokens()
        || counts.output_tokens() < old_counts.output_tokens()
        || new_numerator < old_numerator
    {
        return Err(ValidationError::new(
            ValidationCode::InconsistentUsage,
            "cumulative counts or exact recognized cost regressed",
        ));
    }
    let delta = new_numerator.checked_sub(old_numerator).ok_or_else(|| {
        ValidationError::new(
            ValidationCode::InconsistentUsage,
            "recognized cost regressed",
        )
    })?;
    let remaining = previous
        .remaining_numerator
        .checked_sub(delta)
        .ok_or_else(|| {
            ValidationError::new(
                ValidationCode::UncoveredUsage,
                "reported usage exceeds remaining funded reservation",
            )
        })?;
    let released = if final_status.is_some() {
        remaining
    } else {
        U256::ZERO
    };
    let state = RunAccounting::new(
        counts,
        if final_status.is_some() {
            U256::ZERO
        } else {
            remaining
        },
        final_status,
    )?;
    Ok(AccountingTransition {
        state,
        delta_numerator: delta,
        released_numerator: released,
        duplicate: counts == old_counts && final_status.is_none(),
    })
}
