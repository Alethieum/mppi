use mppi_provider::{Address, B256, RunStatus, TokenCounts, interfaces::*, types::*};

fn context() -> RunContext {
    RunContext::new(B256::ZERO, B256::ZERO, 1, "example/model", B256::ZERO).unwrap()
}

#[test]
fn validated_types_reject_unsafe_representations() {
    assert!(RunContext::new(B256::ZERO, B256::ZERO, 0, "example/model", B256::ZERO).is_err());
    assert!(RunContext::new(B256::ZERO, B256::ZERO, 1, "invalid", B256::ZERO).is_err());
    assert!(Domain::new(0, Address::repeat_byte(1)).is_err());
    assert!(Domain::new(1, Address::ZERO).is_err());
    let observation = ChainObservation::new(1, Address::repeat_byte(1), 0, B256::ZERO).unwrap();
    assert!(ConfirmedSettlementRef::new(observation, B256::ZERO, B256::ZERO, 1, 0).is_err());
    assert!(ConfirmedSettlementRef::new(observation, B256::ZERO, B256::ZERO, 1, 2).is_err());
    let settlement =
        ConfirmedSettlementRef::new(observation, B256::ZERO, B256::ZERO, 2, 1).unwrap();
    assert!(
        SessionOperationOutcome::new(B256::repeat_byte(1), None, None, Some(settlement), None)
            .is_err()
    );
    assert!(OperationResult::new("", OperationState::Unknown, None, None, None, vec![]).is_err());
    assert!(
        OperationResult::new(
            "op",
            OperationState::Confirmed,
            None,
            Some(B256::ZERO),
            None,
            vec![]
        )
        .is_err()
    );
    let result = OperationResult::new(
        "op",
        OperationState::Unknown,
        Some("retained-wallet-id".into()),
        Some(B256::ZERO),
        None,
        vec![],
    )
    .unwrap();
    assert_eq!(result.state(), OperationState::Unknown);
    assert_eq!(result.wallet_operation_id(), Some("retained-wallet-id"));
    assert!(
        OperationResult::new(
            "op",
            OperationState::Unknown,
            Some(String::new()),
            None,
            None,
            vec![]
        )
        .is_err()
    );
    assert!(validate_page_limit(0).is_err());
    assert_eq!(validate_page_limit(1000).unwrap(), 1000);
    assert!(validate_page_limit(1001).is_err());
}

#[test]
fn prepared_request_preserves_bytes_and_finals_require_admission() {
    let context = context();
    let payload = br#" {"model":"example/model","seed":9007199254740993} "#.to_vec();
    let prepared = PreparedRequest::new(payload.clone(), false, &context).unwrap();
    assert_eq!(prepared.payload(), payload);
    assert!(!prepared.stream());
    assert!(PreparedRequest::new(payload, true, &context).is_err());
    let counts = TokenCounts::new(0, 0, 0).unwrap();
    assert!(AccountingResult::new(context.clone(), counts, 0, 0, false).is_err());
    let result = AccountingResult::new(context, counts, 1, 0, false).unwrap();
    assert!(FinalAccountingResult::new(result.clone(), RunStatus::NotAdmitted).is_err());
    assert_eq!(
        FinalAccountingResult::new(result, RunStatus::Completed)
            .unwrap()
            .status(),
        RunStatus::Completed
    );
}

#[test]
fn unknown_not_ready_reasons_cannot_become_grants() {
    let failure = PaymentFailure::new(
        PaymentCode::SessionNotReady,
        Some("future_reason".into()),
        None,
        None,
        None,
        None,
        None,
    )
    .unwrap();
    assert!(Readiness::new(true, vec![failure.clone()]).is_err());
    assert!(CoverageResult::new(true, 0, Some(failure.clone())).is_err());
    assert!(
        !Readiness::new(false, vec![failure.clone()])
            .unwrap()
            .ready()
    );
    assert!(
        !CoverageResult::new(false, 0, Some(failure))
            .unwrap()
            .granted()
    );
}

#[tonic::async_trait]
impl CoverageProvider for Counter {
    async fn coverage(
        &self,
        request: &PreparedRequest,
        _: &RunContext,
    ) -> IntegrationResult<TokenCounts> {
        assert_eq!(request.payload(), br#"{"model":"example/model"}"#);
        Ok(TokenCounts::new(12, 4, 8)?)
    }
}
struct Counter;
#[tokio::test]
async fn provider_hook_is_native_async_and_object_safe() {
    let hook: Box<dyn CoverageProvider> = Box::new(Counter);
    let context = context();
    let request =
        PreparedRequest::new(br#"{"model":"example/model"}"#.to_vec(), false, &context).unwrap();
    assert_eq!(
        hook.coverage(&request, &context).await.unwrap(),
        TokenCounts::new(12, 4, 8).unwrap()
    );
}
