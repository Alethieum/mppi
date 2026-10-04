//! Exact MPPI payment calculations in six-decimal USDC base units.
//!
//! Prices are per million tokens. Numerators use checked 256-bit arithmetic;
//! final monetary values are checked before narrowing to `u64`.

use alloy_primitives::U256;

use crate::errors::{ValidationCode, ValidationError};

pub const RATE_SCALE: u64 = 1_000_000;
pub const MAX_UINT64: u64 = u64::MAX;

/// Accepted prices. An absent cache rate differs from an explicit zero rate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rates {
    input: u64,
    output: u64,
    cache: Option<u64>,
}

impl Rates {
    pub const fn new(input: u64, output: u64, cache: Option<u64>) -> Self {
        Self {
            input,
            output,
            cache,
        }
    }

    pub const fn input(self) -> u64 {
        self.input
    }

    pub const fn output(self) -> u64 {
        self.output
    }

    pub const fn cache(self) -> Option<u64> {
        self.cache
    }

    pub fn greatest(self) -> u64 {
        self.input.max(self.output).max(self.cache.unwrap_or(0))
    }
}

/// Provider-supplied cumulative counts; cached input is a subset of input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TokenCounts {
    input_tokens: u64,
    cache_tokens: u64,
    output_tokens: u64,
}

impl TokenCounts {
    pub fn new(
        input_tokens: u64,
        cache_tokens: u64,
        output_tokens: u64,
    ) -> Result<Self, ValidationError> {
        if cache_tokens > input_tokens {
            return Err(invalid("cache_tokens exceeds input_tokens"));
        }
        Ok(Self {
            input_tokens,
            cache_tokens,
            output_tokens,
        })
    }

    pub const fn input_tokens(self) -> u64 {
        self.input_tokens
    }

    pub const fn cache_tokens(self) -> u64 {
        self.cache_tokens
    }

    pub const fn output_tokens(self) -> u64 {
        self.output_tokens
    }
}

fn invalid(message: &'static str) -> ValidationError {
    ValidationError::new(ValidationCode::InvalidValue, message)
}

fn wide_overflow() -> ValidationError {
    invalid("payment numerator exceeds uint256")
}

fn checked_sum(values: impl IntoIterator<Item = U256>) -> Result<U256, ValidationError> {
    values.into_iter().try_fold(U256::ZERO, |sum, value| {
        sum.checked_add(value).ok_or_else(wide_overflow)
    })
}

fn monetary(value: U256) -> Result<u64, ValidationError> {
    u64::try_from(value).map_err(|_| invalid("monetary amount exceeds uint64"))
}

fn product(count: u64, rate: u64) -> Result<U256, ValidationError> {
    U256::from(count)
        .checked_mul(U256::from(rate))
        .ok_or_else(wide_overflow)
}

/// Parse the exact catalog/configuration decimal-string grammar.
pub fn decimal_uint64(value: &str) -> Result<u64, ValidationError> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || bytes.len() > 20
        || (bytes.len() > 1 && bytes[0] == b'0')
        || !bytes.iter().all(u8::is_ascii_digit)
    {
        return Err(invalid("value must be a canonical uint64 decimal string"));
    }
    value
        .parse()
        .map_err(|_| invalid("decimal value exceeds uint64"))
}

/// Price supplied counts without rounding or inventing a missing cache price.
pub fn priced_numerator(counts: TokenCounts, rates: Rates) -> Result<U256, ValidationError> {
    if counts.cache_tokens > 0 && rates.cache.is_none() {
        return Err(ValidationError::new(
            ValidationCode::InconsistentUsage,
            "positive cache count has no accepted cache price",
        ));
    }
    let uncached = counts
        .input_tokens
        .checked_sub(counts.cache_tokens)
        .ok_or_else(|| invalid("cache_tokens exceeds input_tokens"))?;
    checked_sum([
        product(uncached, rates.input)?,
        product(counts.cache_tokens, rates.cache.unwrap_or(0))?,
        product(counts.output_tokens, rates.output)?,
    ])
}

/// Sum every accepted run/model/version numerator, then floor exactly once.
pub fn cumulative_usage(
    numerators: impl IntoIterator<Item = U256>,
) -> Result<u64, ValidationError> {
    monetary(checked_sum(numerators)? / U256::from(RATE_SCALE))
}

/// Sum usage and all remaining/new bounds, then ceil exactly once.
pub fn required_coverage(
    numerators: impl IntoIterator<Item = U256>,
) -> Result<u64, ValidationError> {
    let total = checked_sum(numerators)?;
    let scale = U256::from(RATE_SCALE);
    let whole = monetary(total / scale)?;
    whole
        .checked_add(u64::from(total % scale != U256::ZERO))
        .ok_or_else(|| invalid("required coverage exceeds uint64"))
}

/// Convert caller-selected positive funding tokens at the greatest accepted rate.
pub fn funding_amount(tokens: u64, rates: Rates) -> Result<u64, ValidationError> {
    if tokens == 0 {
        return Err(invalid("funding tokens must be positive"));
    }
    let amount = required_coverage([product(tokens, rates.greatest())?])?;
    if amount == 0 {
        return Err(invalid("funding amount must be positive"));
    }
    Ok(amount)
}

/// Check a positive raw-unit top-up against cumulative deposit capacity.
pub fn top_up_total(deposit: u64, amount: u64) -> Result<u64, ValidationError> {
    if deposit == 0 || amount == 0 {
        return Err(invalid("deposit and top-up amount must be positive"));
    }
    deposit
        .checked_add(amount)
        .ok_or_else(|| invalid("cumulative deposit exceeds uint64"))
}

/// Advisory deposit headroom; all-zero pricing has no token estimate.
///
/// Only this advisory token result saturates at `u64::MAX`. Monetary values
/// and overflowing wide intermediates always reject instead of clamping.
pub fn remaining_funding_tokens(
    deposit: u64,
    usage_numerator: U256,
    reservation_numerators: impl IntoIterator<Item = U256>,
    rates: Rates,
) -> Result<Option<u64>, ValidationError> {
    let reserved = checked_sum(reservation_numerators)?;
    let greatest = rates.greatest();
    if greatest == 0 {
        return Ok(None);
    }
    let committed = usage_numerator
        .checked_add(reserved)
        .ok_or_else(wide_overflow)?;
    let funded = product(deposit, RATE_SCALE)?;
    let available = if committed >= funded {
        U256::ZERO
    } else {
        funded.checked_sub(committed).ok_or_else(wide_overflow)?
    };
    let tokens = (available / U256::from(greatest)).min(U256::from(u64::MAX));
    Ok(Some(monetary(tokens)?))
}
