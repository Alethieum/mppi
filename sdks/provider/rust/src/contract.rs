//! Typed, immutable nonpayable calls generated from the published contract.
//! Preparing calldata grants neither execution permission nor payment evidence.

use crate::capabilities::{FundingAuthorization, WalletCall};
use crate::errors::{ValidationCode, ValidationError};
use crate::signing::SessionDescriptor;
use crate::types::{Domain, validate_descriptor};
use alloy_primitives::{Address, B256, U256, address};
use alloy_sol_types::SolCall;

/// Exact packaged copy of contracts/src/IMPPI.sol; checked by foundation gates.
#[allow(clippy::too_many_arguments)] // Generated event fields match the public Solidity ABI.
pub mod abi {
    alloy_sol_types::sol!(
        #[sol(all_derives)]
        "src/abi/IMPPI.sol"
    );
}
use abi::IMPPI;

pub const ARC_USDC: Address = address!("3600000000000000000000000000000000000000");

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OperationKind {
    Open,
    TopUp,
    Settle,
    SettleBatch,
    Close,
    RequestClose,
    Withdraw,
    ReleaseIdle,
}

#[derive(Clone, Debug)]
pub struct PreparedOperation {
    kind: OperationKind,
    call: WalletCall,
    descriptors: Vec<SessionDescriptor>,
    amounts: Vec<u64>,
    expected_rate_card_hash: Option<B256>,
}
impl PreparedOperation {
    pub fn kind(&self) -> OperationKind {
        self.kind
    }
    pub fn call(&self) -> &WalletCall {
        &self.call
    }
    pub fn descriptors(&self) -> &[SessionDescriptor] {
        &self.descriptors
    }
    pub fn amounts(&self) -> &[u64] {
        &self.amounts
    }
    pub fn expected_rate_card_hash(&self) -> Option<B256> {
        self.expected_rate_card_hash
    }
    pub fn session_ids(&self) -> Vec<B256> {
        let context = crate::signing::SigningContext {
            chain_id: self.call.chain_id(),
            verifying_contract: self.call.to(),
        };
        self.descriptors
            .iter()
            .map(|d| context.session_id(d))
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct ContractCalls {
    domain: Domain,
    token_domain_name: String,
    token_domain_version: String,
}

fn invalid() -> ValidationError {
    ValidationError::new(
        ValidationCode::InvalidValue,
        "call inputs do not match MPPI funding or proof terms",
    )
}

fn descriptor(d: &SessionDescriptor) -> Result<IMPPI::SessionDescriptor, ValidationError> {
    validate_descriptor(d)?;
    if d.token != ARC_USDC {
        return Err(invalid());
    }
    Ok(IMPPI::SessionDescriptor {
        agent: d.agent,
        sessionSigner: d.sessionSigner,
        providerId: d.providerId,
        recipient: d.recipient,
        operator: d.operator,
        token: d.token,
        salt: d.salt,
    })
}

impl ContractCalls {
    pub fn new(
        domain: Domain,
        token_domain_name: String,
        token_domain_version: String,
    ) -> Result<Self, ValidationError> {
        if token_domain_name.is_empty() || token_domain_version.is_empty() {
            return Err(invalid());
        }
        Ok(Self {
            domain,
            token_domain_name,
            token_domain_version,
        })
    }
    pub fn domain(&self) -> &Domain {
        &self.domain
    }
    fn prepare<C: SolCall>(
        &self,
        kind: OperationKind,
        call: C,
        descriptors: Vec<SessionDescriptor>,
        amounts: Vec<u64>,
        expected_rate_card_hash: Option<B256>,
    ) -> Result<PreparedOperation, ValidationError> {
        Ok(PreparedOperation {
            kind,
            call: WalletCall::new(
                self.domain.chain_id(),
                self.domain.verifying_contract(),
                call.abi_encode(),
            )?,
            descriptors,
            amounts,
            expected_rate_card_hash,
        })
    }
    fn authorization(
        &self,
        d: &SessionDescriptor,
        amount: u64,
        auth: &FundingAuthorization,
        signature: &[u8],
    ) -> Result<IMPPI::EIP3009Auth, ValidationError> {
        if amount == 0
            || auth.chain_id() != self.domain.chain_id()
            || auth.token() != ARC_USDC
            || auth.domain_name() != self.token_domain_name
            || auth.domain_version() != self.token_domain_version
            || auth.owner() != d.agent
            || auth.recipient() != self.domain.verifying_contract()
            || auth.value() != U256::from(amount)
            || signature.is_empty()
            || signature.len() > 65536
        {
            return Err(invalid());
        }
        Ok(IMPPI::EIP3009Auth {
            validAfter: auth.valid_after(),
            validBefore: auth.valid_before(),
            nonce: auth.nonce(),
            signature: signature.to_vec().into(),
        })
    }
    pub fn open(
        &self,
        d: &SessionDescriptor,
        deposit: u64,
        expected_rate_card_hash: B256,
        auth: &FundingAuthorization,
        signature: &[u8],
    ) -> Result<PreparedOperation, ValidationError> {
        self.prepare(
            OperationKind::Open,
            IMPPI::openCall {
                d: descriptor(d)?,
                deposit,
                expectedRateCardHash: expected_rate_card_hash,
                auth: self.authorization(d, deposit, auth, signature)?,
            },
            vec![d.clone()],
            vec![deposit],
            Some(expected_rate_card_hash),
        )
    }
    pub fn top_up(
        &self,
        d: &SessionDescriptor,
        amount: u64,
        auth: &FundingAuthorization,
        signature: &[u8],
    ) -> Result<PreparedOperation, ValidationError> {
        self.prepare(
            OperationKind::TopUp,
            IMPPI::topUpCall {
                d: descriptor(d)?,
                amount,
                auth: self.authorization(d, amount, auth, signature)?,
            },
            vec![d.clone()],
            vec![amount],
            None,
        )
    }
    fn capture(
        &self,
        kind: OperationKind,
        d: &SessionDescriptor,
        cumulative: u64,
        capture: u64,
        signature: &[u8],
    ) -> Result<PreparedOperation, ValidationError> {
        if capture > cumulative
            || !(signature.len() == 65 || (kind == OperationKind::Close && signature.is_empty()))
        {
            return Err(invalid());
        }
        let desc = descriptor(d)?;
        if kind == OperationKind::Close {
            self.prepare(
                kind,
                IMPPI::closeCall {
                    d: desc,
                    cumulativeAmount: cumulative,
                    captureAmount: capture,
                    voucherSig: signature.to_vec().into(),
                },
                vec![d.clone()],
                vec![capture],
                None,
            )
        } else {
            self.prepare(
                kind,
                IMPPI::settleCall {
                    d: desc,
                    cumulativeAmount: cumulative,
                    captureAmount: capture,
                    voucherSig: signature.to_vec().into(),
                },
                vec![d.clone()],
                vec![capture],
                None,
            )
        }
    }
    pub fn settle(
        &self,
        d: &SessionDescriptor,
        cumulative: u64,
        capture: u64,
        signature: &[u8],
    ) -> Result<PreparedOperation, ValidationError> {
        self.capture(OperationKind::Settle, d, cumulative, capture, signature)
    }
    pub fn close(
        &self,
        d: &SessionDescriptor,
        cumulative: u64,
        capture: u64,
        signature: &[u8],
    ) -> Result<PreparedOperation, ValidationError> {
        self.capture(OperationKind::Close, d, cumulative, capture, signature)
    }
    pub fn settle_batch(
        &self,
        items: &[(&SessionDescriptor, u64, u64, &[u8])],
    ) -> Result<PreparedOperation, ValidationError> {
        let mut call = IMPPI::settleBatchCall {
            d: Vec::new(),
            cumulativeAmounts: Vec::new(),
            captureAmounts: Vec::new(),
            voucherSigs: Vec::new(),
        };
        for (d, cumulative, capture, sig) in items {
            if capture > cumulative || sig.len() != 65 {
                return Err(invalid());
            }
            call.d.push(descriptor(d)?);
            call.cumulativeAmounts.push(*cumulative);
            call.captureAmounts.push(*capture);
            call.voucherSigs.push(sig.to_vec().into());
        }
        self.prepare(
            OperationKind::SettleBatch,
            call,
            items.iter().map(|i| i.0.clone()).collect(),
            items.iter().map(|i| i.2).collect(),
            None,
        )
    }
    pub fn request_close(
        &self,
        d: &SessionDescriptor,
    ) -> Result<PreparedOperation, ValidationError> {
        self.prepare(
            OperationKind::RequestClose,
            IMPPI::requestCloseCall { d: descriptor(d)? },
            vec![d.clone()],
            vec![],
            None,
        )
    }
    pub fn withdraw(&self, d: &SessionDescriptor) -> Result<PreparedOperation, ValidationError> {
        self.prepare(
            OperationKind::Withdraw,
            IMPPI::withdrawCall { d: descriptor(d)? },
            vec![d.clone()],
            vec![],
            None,
        )
    }
    pub fn release_idle(
        &self,
        d: &SessionDescriptor,
    ) -> Result<PreparedOperation, ValidationError> {
        self.prepare(
            OperationKind::ReleaseIdle,
            IMPPI::releaseIdleCall { d: descriptor(d)? },
            vec![d.clone()],
            vec![],
            None,
        )
    }
}
