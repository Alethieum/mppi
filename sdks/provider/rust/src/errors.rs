//! Local validation failures; never evidence of a chain transaction outcome.

use core::fmt;

/// Stable machine-readable validation codes shared with the Python SDK.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValidationCode {
    InvalidValue,
    LimitExceeded,
    NonCanonical,
    DuplicateKey,
    InconsistentUsage,
    UncoveredUsage,
}

impl ValidationCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidValue => "INVALID_VALUE",
            Self::LimitExceeded => "LIMIT_EXCEEDED",
            Self::NonCanonical => "NON_CANONICAL",
            Self::DuplicateKey => "DUPLICATE_KEY",
            Self::InconsistentUsage => "INCONSISTENT_USAGE",
            Self::UncoveredUsage => "UNCOVERED_USAGE",
        }
    }
}

/// Rejected local input with a stable code and a bounded, non-sensitive message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValidationError {
    code: ValidationCode,
    message: &'static str,
}

impl ValidationError {
    pub(crate) const fn new(code: ValidationCode, message: &'static str) -> Self {
        Self { code, message }
    }

    pub const fn code(&self) -> ValidationCode {
        self.code
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for ValidationError {}
