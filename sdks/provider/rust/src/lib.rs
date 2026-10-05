//! MPPI provider payment runtime, validated interfaces and chain evidence.
//!
//! Exact catalog, amount and report calculations operate on provider-supplied
//! data. They do not establish that a session may execute. Configuration,
//! discovery, read-only chain following and explicit wallet operations are
//! implemented. [`runtime::Mppi`] composes native paid services and lifecycle
//! with caller-supplied execution, counts and wallet capabilities.

pub use alloy_primitives::{Address, B256, U256};

pub mod accounting;
pub mod amounts;
pub mod capabilities;
pub mod catalog;
pub mod chain;
pub mod circle;
pub mod cli;
pub mod config;
pub mod contract;
pub mod deployment;
pub mod discovery;
pub mod errors;
pub mod follower;
pub mod http;
pub mod interfaces;
mod json;
mod local_state;
pub mod onboarding;
pub mod operations;
pub mod provider_update;
pub mod queue;
pub mod registry;
pub mod runtime;
pub mod setup;
pub mod transport;
pub mod types;
pub mod verify;
pub mod wire;

/// Generated from the public `mppi.v1` protobuf schema; normal builds need no protoc.
pub mod proto {
    include!("generated/mppi.v1.rs");
}

pub mod signing;
pub mod standalone;

pub use accounting::{AccountingTransition, RunAccounting, RunStatus, report_transition};
pub use amounts::{
    MAX_UINT64, RATE_SCALE, Rates, TokenCounts, cumulative_usage, decimal_uint64, funding_amount,
    priced_numerator, remaining_funding_tokens, required_coverage, top_up_total,
};
pub use catalog::{
    Catalog, CatalogRow, DEFAULT_CATALOG_BYTES, DEFAULT_JSON_DEPTH, parse_catalog, publish_catalog,
    validate_model,
};
pub use errors::{ValidationCode, ValidationError};
