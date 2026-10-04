//! Read-only checks of a provider's published HTTPS origin and native services.
//!
//! Verification never opens a funded session, signs a voucher or invokes a
//! wallet. It observes identity/discovery consistency and an unpaid duplex
//! exchange; funded execution and provider infrastructure need separate tests.

use std::{
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use futures_core::Stream;
use tokio::sync::mpsc;
use tonic::{
    Request, Status,
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};

use crate::{
    B256,
    chain::ChainReader,
    config::ProviderConfig,
    discovery::{
        DiscoveryDocuments, MAX_DOCUMENT_BYTES, ProviderIdentity, parse_document, verify_models,
        verify_registration,
    },
    http::HttpTransport,
    proto,
    types::PauseLevel,
    wire,
};

/// A bounded, sanitized failure stage. Remote errors and URLs are not retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VerificationError {
    Policy,
    Deadline,
    Chain,
    Identity,
    ContractPaused,
    DiscoveryTransport,
    DiscoveryContent,
    GrpcTransport,
    Control,
    Challenge,
    Complete,
    Entropy,
}
impl fmt::Display for VerificationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Policy => "invalid endpoint verification policy",
            Self::Deadline => "endpoint verification deadline elapsed",
            Self::Chain => "deployment or canonical chain evidence unavailable",
            Self::Identity => "configured provider differs from confirmed registry state",
            Self::ContractPaused => "MPPI contract is paused",
            Self::DiscoveryTransport => "HTTPS discovery unavailable or invalid",
            Self::DiscoveryContent => {
                "published discovery differs from confirmed identity or applied configuration"
            }
            Self::GrpcTransport => "TLS native gRPC connection unavailable",
            Self::Control => "native Control duplex exchange failed",
            Self::Challenge => "Control challenge differs from confirmed provider configuration",
            Self::Complete => {
                "unpaid Complete did not return the required authentication rejection"
            }
            Self::Entropy => "verification request identifiers unavailable",
        })
    }
}
impl std::error::Error for VerificationError {}

/// One total deadline and one trust policy for HTTP discovery and native gRPC.
/// A supplied PEM bundle replaces system roots. Hostname verification remains
/// enabled, and the public origin always uses HTTPS. The maximum is 60 seconds.
#[derive(Clone)]
pub struct VerifyPolicy {
    timeout: Duration,
    ca_pem: Option<Vec<u8>>,
    http: HttpTransport,
}
impl fmt::Debug for VerifyPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifyPolicy")
            .field("timeout", &self.timeout)
            .field("custom_trust_roots", &self.ca_pem.is_some())
            .finish_non_exhaustive()
    }
}
impl VerifyPolicy {
    pub fn new(timeout_ms: u64, ca_pem: Option<&[u8]>) -> Result<Self, VerificationError> {
        if !(1..=60_000).contains(&timeout_ms) {
            return Err(VerificationError::Policy);
        }
        let http = HttpTransport::new(timeout_ms, MAX_DOCUMENT_BYTES, ca_pem)
            .map_err(|_| VerificationError::Policy)?;
        Ok(Self {
            timeout: Duration::from_millis(timeout_ms),
            ca_pem: ca_pem.map(<[u8]>::to_vec),
            http,
        })
    }
}

/// Successful observations, scoped to the returned canonical registry snapshot.
/// This report does not certify future readiness or a provider's infrastructure.
#[derive(Clone, Debug)]
pub struct VerificationReport {
    identity: ProviderIdentity,
}
impl VerificationReport {
    pub fn identity(&self) -> &ProviderIdentity {
        &self.identity
    }
    pub fn checks(&self) -> &'static [&'static str] {
        &[
            "deployment_and_canonical_identity",
            "contract_unpaused",
            "https_discovery_and_applied_configuration",
            "tls_native_grpc",
            "control_responses_before_request_eof",
            "correlated_challenges_match_confirmed_catalog",
            "concurrent_complete_unauthenticated_status",
            "control_remains_usable_after_complete_rejection",
        ]
    }
    pub fn not_tested(&self) -> &'static [&'static str] {
        &[
            "wallet_access_and_transaction_submission",
            "funded_admission_streaming_and_rich_payment_errors",
            "provider_usage_measurement_and_job_termination",
            "remote_cancellation_cleanup_and_resource_bounds",
            "cross_replica_state_and_recovery",
        ]
    }
}

/// Verify the configured public origin without funding, signing or submitting.
/// The caller separately checks its selected wallet access. A failed check
/// returns its sanitized stage; no partial result is labelled endpoint-ready.
pub async fn verify_endpoint(
    config: &ProviderConfig,
    reader: &ChainReader,
    policy: &VerifyPolicy,
) -> Result<VerificationReport, VerificationError> {
    tokio::time::timeout(policy.timeout, verify(config, reader, policy))
        .await
        .map_err(|_| VerificationError::Deadline)?
}

async fn verify(
    config: &ProviderConfig,
    reader: &ChainReader,
    policy: &VerifyPolicy,
) -> Result<VerificationReport, VerificationError> {
    if config.chain().chain_id() != reader.profile().domain().chain_id() {
        return Err(VerificationError::Identity);
    }
    let block = reader
        .block(None)
        .await
        .map_err(|_| VerificationError::Chain)?;
    reader
        .verify_deployment(&block)
        .await
        .map_err(|_| VerificationError::Chain)?;
    if reader
        .pause_level(&block)
        .await
        .map_err(|_| VerificationError::Chain)?
        != PauseLevel::None
    {
        return Err(VerificationError::ContractPaused);
    }
    let identity = reader
        .identity(config.identity().provider_id(), &block, false)
        .await
        .map_err(|_| VerificationError::Identity)?;
    let expected =
        DiscoveryDocuments::new(config, &identity).map_err(|_| VerificationError::Identity)?;
    let origin = config.provider().public_origin();
    let registration_url = format!("{origin}/discover");
    let alias_url = format!("{origin}/.well-known/agent-registration.json");
    let models_url = format!("{origin}/models");
    let (registration, alias, models) = tokio::try_join!(
        policy.http.request(&registration_url, None),
        policy.http.request(&alias_url, None),
        policy.http.request(&models_url, None),
    )
    .map_err(|_| VerificationError::DiscoveryTransport)?;
    check_documents(config, &identity, &expected, &registration, &alias, &models)?;

    let tls = match &policy.ca_pem {
        Some(pem) => ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pem)),
        None => ClientTlsConfig::new().with_native_roots(),
    }
    .timeout(policy.timeout);
    let endpoint = Endpoint::from_shared(origin.to_owned())
        .map_err(|_| VerificationError::GrpcTransport)?
        .connect_timeout(policy.timeout)
        .tls_config(tls)
        .map_err(|_| VerificationError::GrpcTransport)?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|_| VerificationError::GrpcTransport)?;
    probe(channel, config, &identity, policy.timeout).await?;
    reader
        .check_canonical(&block)
        .await
        .map_err(|_| VerificationError::Chain)?;
    Ok(VerificationReport { identity })
}

fn check_documents(
    config: &ProviderConfig,
    identity: &ProviderIdentity,
    expected: &DiscoveryDocuments,
    registration: &[u8],
    alias: &[u8],
    models: &[u8],
) -> Result<(), VerificationError> {
    let catalog = identity.catalog().ok_or(VerificationError::Identity)?;
    let check = || {
        verify_registration(
            registration,
            identity,
            Some(config.provider().public_origin()),
        )?;
        verify_registration(alias, identity, Some(config.provider().public_origin()))?;
        verify_models(models, catalog)?;
        let published = parse_document(registration, MAX_DOCUMENT_BYTES)?;
        Ok::<_, crate::ValidationError>(
            published == parse_document(alias, MAX_DOCUMENT_BYTES)?
                && published == parse_document(expected.registration(), MAX_DOCUMENT_BYTES)?
                && parse_document(models, MAX_DOCUMENT_BYTES)?
                    == parse_document(expected.models(), MAX_DOCUMENT_BYTES)?,
        )
    };
    if check().map_err(|_| VerificationError::DiscoveryContent)? {
        Ok(())
    } else {
        Err(VerificationError::DiscoveryContent)
    }
}

struct Requests(mpsc::Receiver<proto::ControlMessage>);
impl Stream for Requests {
    type Item = proto::ControlMessage;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.0.poll_recv(cx)
    }
}

fn random_id() -> Result<B256, VerificationError> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|_| VerificationError::Entropy)?;
    Ok(B256::from(bytes))
}

fn check_challenge(
    message: proto::ControlMessage,
    config: &ProviderConfig,
    identity: &ProviderIdentity,
    request_id: B256,
    model: &str,
) -> Result<(), VerificationError> {
    wire::validate_control_direction(&message, wire::ControlSender::Provider)
        .map_err(|_| VerificationError::Challenge)?;
    let Some(proto::control_message::Kind::Challenge(challenge)) = message.kind else {
        return Err(VerificationError::Challenge);
    };
    let catalog = identity.catalog().ok_or(VerificationError::Challenge)?;
    if challenge.request_id.as_deref() != Some(request_id.as_slice())
        || challenge.model.as_deref() != Some(model)
        || challenge.rate_card_hash.as_deref() != Some(catalog.rate_card_hash().as_slice())
        || challenge.provider_id.as_deref()
            != Some(identity.provider_id().to_be_bytes::<32>().as_slice())
        || challenge.contract.as_deref()
            != Some(identity.observation().contract_address().as_slice())
        || challenge.chain_id != Some(identity.observation().chain_id())
        || challenge.operator.as_deref() != Some(identity.owner().as_slice())
        || challenge.cover_output_tokens != Some(config.payment().output_buffer_tokens())
        || challenge.suggest_session_tokens != Some(config.payment().suggest_session_tokens())
        || catalog.rates_for(model).is_none()
    {
        return Err(VerificationError::Challenge);
    }
    Ok(())
}

async fn challenge(
    input: &mpsc::Sender<proto::ControlMessage>,
    output: &mut tonic::Streaming<proto::ControlMessage>,
    config: &ProviderConfig,
    identity: &ProviderIdentity,
    model: &str,
) -> Result<(), VerificationError> {
    let id = random_id()?;
    input
        .send(proto::ControlMessage {
            kind: Some(proto::control_message::Kind::ChallengeRequest(
                proto::ChallengeRequest {
                    request_id: Some(id.to_vec()),
                    model: Some(model.to_owned()),
                },
            )),
        })
        .await
        .map_err(|_| VerificationError::Control)?;
    let message = output
        .message()
        .await
        .map_err(|_| VerificationError::Control)?
        .ok_or(VerificationError::Control)?;
    check_challenge(message, config, identity, id, model)
}

async fn probe(
    channel: Channel,
    config: &ProviderConfig,
    identity: &ProviderIdentity,
    timeout: Duration,
) -> Result<(), VerificationError> {
    let model = config
        .models()
        .first()
        .ok_or(VerificationError::Identity)?
        .model();
    let mut control = proto::control_client::ControlClient::new(channel.clone())
        .max_decoding_message_size(wire::MAX_CONTROL_BYTES)
        .max_encoding_message_size(wire::MAX_CONTROL_BYTES);
    let mut chat = proto::chat_client::ChatClient::new(channel)
        .max_decoding_message_size(wire::MAX_CONTROL_BYTES)
        .max_encoding_message_size(wire::MAX_CONTROL_BYTES);
    let (input, requests) = mpsc::channel(1);
    let mut request = Request::new(Requests(requests));
    request.set_timeout(timeout);
    let mut output = control
        .session(request)
        .await
        .map_err(|_| VerificationError::Control)?
        .into_inner();
    let nonce = output
        .message()
        .await
        .map_err(|_| VerificationError::Control)?
        .ok_or(VerificationError::Control)?;
    wire::validate_control_direction(&nonce, wire::ControlSender::Provider)
        .map_err(|_| VerificationError::Control)?;
    if !matches!(
        nonce.kind,
        Some(proto::control_message::Kind::ReconnectChallenge(_))
    ) {
        return Err(VerificationError::Control);
    }
    // Structurally valid but unauthenticated: no existing session authority,
    // signer, voucher, funding authorization or provider execution is supplied.
    let mut unpaid = Request::new(proto::CompleteRequest {
        model: Some(model.to_owned()),
        stream: Some(true),
        session_id: Some(random_id()?.to_vec()),
        run_id: Some(random_id()?.to_vec()),
        control_generation: Some(1),
        rate_card_hash: Some(config.catalog().rate_card_hash().to_vec()),
        auth_signature: Some(vec![0; 65]),
        payload: Some(
            serde_json::to_vec(&serde_json::json!({"model":model,"stream":true,"messages":[]}))
                .map_err(|_| VerificationError::Complete)?,
        ),
    });
    unpaid.set_timeout(timeout);
    let complete = async {
        let status = match chat.completions(unpaid).await {
            Err(status) => status,
            Ok(response) => match response.into_inner().message().await {
                Err(status) => status,
                Ok(_) => return Err(VerificationError::Complete),
            },
        };
        check_rejection(&status)
    };
    tokio::try_join!(
        challenge(&input, &mut output, config, identity, model),
        complete
    )?;
    // Keep the input stream open throughout Complete. A second correlated reply
    // proves that the same Control stream remained usable across its rejection.
    challenge(&input, &mut output, config, identity, model).await?;
    // Drop sends local cancellation. No claim is made about remote job cleanup.
    drop(output);
    drop(input);
    Ok(())
}

fn check_rejection(status: &Status) -> Result<(), VerificationError> {
    if status.code() != tonic::Code::Unauthenticated || !status.details().is_empty() {
        return Err(VerificationError::Complete);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Address, U256,
        chain::{ChainError, CodePin, DeploymentProfile, Rpc},
        types::{ChainObservation, Domain},
    };
    use serde_json::{Value, json};
    use std::{
        io,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };
    use tokio::{
        net::{TcpListener, TcpStream},
        sync::Notify,
    };
    use tokio_util::sync::CancellationToken;
    use tonic::{Response, Streaming};

    fn fixture() -> (ProviderConfig, ProviderIdentity) {
        let config = ProviderConfig::from_mapping(&json!({
            "provider":{"name":"Verification fixture","description":"Synthetic declarations","public_origin":"https://provider.example","recipient":"0x2222222222222222222222222222222222222222"},
            "identity":{"provider_id":"42","wallet_address":"0x1111111111111111111111111111111111111111"},
            "chain":{"network":"testnet","max_age_ms":1000},
            "upstream":{"url":"http://127.0.0.1:8080/v1"},
            "models":[{"model":"example/model","description":"Text","context_window":8000,"max_output":1000,"modalities":["text"],"embeddings":false,"streaming":true,"tools":false,"structured_output":false,"rates":{"input":"1","output":"2"}}],
            "payment":{"collection_margin_ms":60000,"authorization_wait_ms":10000},
            "settlement":{"interval_ms":30000}
        }), None).unwrap();
        let identity = ProviderIdentity::new(
            ChainObservation::new(5042002, Address::repeat_byte(3), 1, B256::repeat_byte(4))
                .unwrap(),
            Address::repeat_byte(5),
            U256::from(42),
            config.identity().wallet_address(),
            config.provider().recipient(),
            "https://provider.example/discover".into(),
            Some(config.catalog().clone()),
        )
        .unwrap();
        (config, identity)
    }

    fn challenge_for(
        config: &ProviderConfig,
        identity: &ProviderIdentity,
        id: Vec<u8>,
    ) -> proto::ControlMessage {
        proto::ControlMessage {
            kind: Some(proto::control_message::Kind::Challenge(proto::Challenge {
                request_id: Some(id),
                model: Some("example/model".into()),
                rate_card_hash: Some(identity.catalog().unwrap().rate_card_hash().to_vec()),
                provider_id: Some(identity.provider_id().to_be_bytes::<32>().to_vec()),
                contract: Some(identity.observation().contract_address().to_vec()),
                chain_id: Some(identity.observation().chain_id()),
                operator: Some(identity.owner().to_vec()),
                cover_output_tokens: Some(config.payment().output_buffer_tokens()),
                suggest_session_tokens: Some(config.payment().suggest_session_tokens()),
            })),
        }
    }

    #[test]
    fn rejects_invalid_policy_and_redacts_configuration() {
        for duration in [0, 60_001, u64::MAX] {
            assert_eq!(
                VerifyPolicy::new(duration, None).unwrap_err(),
                VerificationError::Policy
            );
        }
        assert_eq!(
            VerifyPolicy::new(1000, Some(b"secret-invalid-ca")).unwrap_err(),
            VerificationError::Policy
        );
        assert_eq!(
            VerifyPolicy::new(1000, Some(&vec![b'x'; MAX_DOCUMENT_BYTES + 1])).unwrap_err(),
            VerificationError::Policy
        );
        let policy = VerifyPolicy::new(1000, None).unwrap();
        assert!(!format!("{policy:?}").contains("provider.example"));
        for error in [
            VerificationError::DiscoveryTransport,
            VerificationError::GrpcTransport,
            VerificationError::Complete,
        ] {
            assert!(!error.to_string().contains("secret"));
        }
    }

    #[test]
    fn discovery_checks_alias_config_and_original_duplicate_keys() {
        let (config, identity) = fixture();
        let documents = DiscoveryDocuments::new(&config, &identity).unwrap();
        let registration = documents.registration();
        let models = documents.models();
        check_documents(
            &config,
            &identity,
            &documents,
            registration,
            registration,
            models,
        )
        .unwrap();
        let pretty =
            serde_json::to_vec_pretty(&parse_document(registration, MAX_DOCUMENT_BYTES).unwrap())
                .unwrap();
        check_documents(
            &config,
            &identity,
            &documents,
            &pretty,
            registration,
            models,
        )
        .unwrap();
        let mut changed: Value = serde_json::from_slice(registration).unwrap();
        changed["name"] = json!("Unapplied declaration");
        let changed = serde_json::to_vec(&changed).unwrap();
        for (a, b) in [
            (&changed[..], registration),
            (registration, &changed[..]),
            (&changed[..], &changed[..]),
        ] {
            assert_eq!(
                check_documents(&config, &identity, &documents, a, b, models),
                Err(VerificationError::DiscoveryContent)
            );
        }
        let mut bad_models: Value = serde_json::from_slice(models).unwrap();
        bad_models["models"][0]["description"] = json!("Unapplied capabilities");
        assert_eq!(
            check_documents(
                &config,
                &identity,
                &documents,
                registration,
                registration,
                &serde_json::to_vec(&bad_models).unwrap()
            ),
            Err(VerificationError::DiscoveryContent)
        );
        let duplicate = format!(
            "{{\"name\":\"injected\",{}",
            std::str::from_utf8(registration)
                .unwrap()
                .trim_start_matches('{')
        );
        assert_eq!(
            check_documents(
                &config,
                &identity,
                &documents,
                duplicate.as_bytes(),
                registration,
                models
            ),
            Err(VerificationError::DiscoveryContent)
        );
    }

    #[test]
    fn every_challenge_binding_and_guidance_must_match() {
        let (config, identity) = fixture();
        let id = B256::repeat_byte(8);
        check_challenge(
            challenge_for(&config, &identity, id.to_vec()),
            &config,
            &identity,
            id,
            "example/model",
        )
        .unwrap();
        for field in 0..10 {
            let mut message = challenge_for(&config, &identity, id.to_vec());
            let Some(proto::control_message::Kind::Challenge(value)) = message.kind.as_mut() else {
                unreachable!()
            };
            match field {
                0 => value.request_id = Some(vec![9; 32]),
                1 => value.model = Some("other/model".into()),
                2 => value.rate_card_hash = Some(vec![9; 32]),
                3 => value.provider_id = Some(vec![9; 32]),
                4 => value.contract = Some(vec![9; 20]),
                5 => value.chain_id = Some(5042),
                6 => value.operator = Some(vec![9; 20]),
                7 => value.cover_output_tokens = Some(1001),
                8 => value.suggest_session_tokens = Some(1),
                _ => value.request_id = None,
            }
            assert_eq!(
                check_challenge(message, &config, &identity, id, "example/model"),
                Err(VerificationError::Challenge)
            );
        }
    }

    #[test]
    fn unpaid_check_rejects_paid_errors_and_false_success_claims() {
        check_rejection(&Status::unauthenticated("untrusted remote message")).unwrap();
        for status in [
            Status::invalid_argument("missing field"),
            Status::permission_denied("denied"),
            Status::unavailable("network"),
            Status::with_details(
                tonic::Code::Unauthenticated,
                "invalid",
                b"unexpected".as_slice().into(),
            ),
        ] {
            assert_eq!(check_rejection(&status), Err(VerificationError::Complete));
        }
        let (_, identity) = fixture();
        let report = VerificationReport { identity };
        assert!(
            report
                .not_tested()
                .contains(&"funded_admission_streaming_and_rich_payment_errors")
        );
        assert!(
            report
                .not_tested()
                .contains(&"wallet_access_and_transaction_submission")
        );
        assert!(
            !report
                .checks()
                .contains(&"provider_usage_measurement_and_job_termination")
        );
    }

    #[derive(Clone, Copy)]
    enum Behavior {
        Good,
        Buffered,
        WrongChallenge,
        WrongStatus,
        Admits,
        MissingNonce,
        ClosesControl,
    }
    #[derive(Clone)]
    struct Peer {
        config: ProviderConfig,
        identity: ProviderIdentity,
        behavior: Behavior,
        completed: Arc<Notify>,
        challenges: Arc<AtomicUsize>,
        completions: Arc<AtomicUsize>,
        premature_eof: Arc<AtomicBool>,
    }
    struct Replies<T>(mpsc::Receiver<Result<T, Status>>);
    impl<T> Stream for Replies<T> {
        type Item = Result<T, Status>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.0.poll_recv(cx)
        }
    }
    #[tonic::async_trait]
    impl proto::control_server::Control for Peer {
        type SessionStream = Replies<proto::ControlMessage>;
        async fn session(
            &self,
            request: Request<Streaming<proto::ControlMessage>>,
        ) -> Result<Response<Self::SessionStream>, Status> {
            let mut input = request.into_inner();
            if matches!(self.behavior, Behavior::Buffered) {
                while input.message().await?.is_some() {}
                self.premature_eof.store(true, Ordering::SeqCst);
            }
            let (send, receive) = mpsc::channel(2);
            let peer = self.clone();
            tokio::spawn(async move {
                let first = if matches!(peer.behavior, Behavior::MissingNonce) {
                    challenge_for(&peer.config, &peer.identity, vec![0; 32])
                } else {
                    proto::ControlMessage {
                        kind: Some(proto::control_message::Kind::ReconnectChallenge(
                            proto::ReconnectChallenge {
                                nonce: Some(vec![1; 32]),
                            },
                        )),
                    }
                };
                if send.send(Ok(first)).await.is_err() {
                    return;
                }
                while let Ok(Some(message)) = input.message().await {
                    let Some(proto::control_message::Kind::ChallengeRequest(value)) = message.kind
                    else {
                        return;
                    };
                    let count = peer.challenges.fetch_add(1, Ordering::SeqCst);
                    // A sequential probe deadlocks: the first challenge reply is
                    // intentionally held until concurrent Complete has arrived.
                    if count == 0 {
                        peer.completed.notified().await;
                    }
                    if matches!(peer.behavior, Behavior::ClosesControl) {
                        return;
                    }
                    let id = if matches!(peer.behavior, Behavior::WrongChallenge) {
                        vec![7; 32]
                    } else {
                        value.request_id.unwrap()
                    };
                    if send
                        .send(Ok(challenge_for(&peer.config, &peer.identity, id)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                if peer.challenges.load(Ordering::SeqCst) < 2 {
                    peer.premature_eof.store(true, Ordering::SeqCst);
                }
            });
            Ok(Response::new(Replies(receive)))
        }
    }
    #[tonic::async_trait]
    impl proto::chat_server::Chat for Peer {
        type CompletionsStream = Replies<proto::ChatChunk>;
        async fn completions(
            &self,
            request: Request<proto::CompleteRequest>,
        ) -> Result<Response<Self::CompletionsStream>, Status> {
            wire::validate_message(request.get_ref()).unwrap();
            assert_eq!(request.get_ref().auth_signature, Some(vec![0; 65]));
            self.completions.fetch_add(1, Ordering::SeqCst);
            self.completed.notify_one();
            if matches!(self.behavior, Behavior::Admits) {
                let (send, receive) = mpsc::channel(1);
                send.send(Ok(proto::ChatChunk {
                    kind: Some(proto::chat_chunk::Kind::Payload(b"{}".to_vec())),
                }))
                .await
                .unwrap();
                return Ok(Response::new(Replies(receive)));
            }
            if matches!(self.behavior, Behavior::WrongStatus) {
                return Err(Status::invalid_argument("secret-remote-error"));
            }
            Err(Status::unauthenticated("session authority unavailable"))
        }
    }
    struct Incoming(TcpListener);
    impl Stream for Incoming {
        type Item = io::Result<TcpStream>;
        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.0
                .poll_accept(cx)
                .map(|result| Some(result.map(|(stream, _)| stream)))
        }
    }
    async fn native_peer(
        behavior: Behavior,
    ) -> (
        Peer,
        Channel,
        CancellationToken,
        tokio::task::JoinHandle<()>,
    ) {
        let (config, identity) = fixture();
        let peer = Peer {
            config,
            identity,
            behavior,
            completed: Arc::new(Notify::new()),
            challenges: Arc::new(AtomicUsize::new(0)),
            completions: Arc::new(AtomicUsize::new(0)),
            premature_eof: Arc::new(AtomicBool::new(false)),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let server_peer = peer.clone();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(proto::control_server::ControlServer::new(
                    server_peer.clone(),
                ))
                .add_service(proto::chat_server::ChatServer::new(server_peer))
                .serve_with_incoming_shutdown(Incoming(listener), stopping.cancelled())
                .await
                .unwrap();
        });
        let channel = Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        (peer, channel, stop, task)
    }
    async fn finish(stop: CancellationToken, task: tokio::task::JoinHandle<()>) {
        stop.cancel();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn native_probe_proves_duplex_and_overlap_on_one_channel() {
        let (peer, channel, stop, task) = native_peer(Behavior::Good).await;
        tokio::time::timeout(
            Duration::from_secs(2),
            probe(
                channel,
                &peer.config,
                &peer.identity,
                Duration::from_secs(1),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(peer.challenges.load(Ordering::SeqCst), 2);
        assert_eq!(peer.completions.load(Ordering::SeqCst), 1);
        assert!(!peer.premature_eof.load(Ordering::SeqCst));
        finish(stop, task).await;
    }

    #[tokio::test]
    async fn native_probe_rejects_wrong_challenge_status_success_and_closed_control() {
        for (behavior, expected) in [
            (Behavior::WrongChallenge, VerificationError::Challenge),
            (Behavior::WrongStatus, VerificationError::Complete),
            (Behavior::Admits, VerificationError::Complete),
            (Behavior::MissingNonce, VerificationError::Control),
            (Behavior::ClosesControl, VerificationError::Control),
        ] {
            let (peer, channel, stop, task) = native_peer(behavior).await;
            assert_eq!(
                tokio::time::timeout(
                    Duration::from_secs(2),
                    probe(
                        channel,
                        &peer.config,
                        &peer.identity,
                        Duration::from_secs(1)
                    )
                )
                .await
                .unwrap(),
                Err(expected)
            );
            finish(stop, task).await;
        }
    }

    #[tokio::test]
    async fn buffered_control_cannot_pass_without_request_eof() {
        let (peer, channel, stop, task) = native_peer(Behavior::Buffered).await;
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                probe(
                    channel,
                    &peer.config,
                    &peer.identity,
                    Duration::from_secs(1)
                )
            )
            .await
            .is_err()
        );
        assert_eq!(peer.challenges.load(Ordering::SeqCst), 0);
        assert_eq!(peer.completions.load(Ordering::SeqCst), 0);
        finish(stop, task).await;
    }

    struct PendingRpc;
    #[tonic::async_trait]
    impl Rpc for PendingRpc {
        async fn call(&self, method: &str, _: Value) -> Result<Value, ChainError> {
            assert_eq!(method, "eth_chainId");
            std::future::pending().await
        }
    }
    #[tokio::test]
    async fn total_deadline_bounds_unresponsive_chain_before_endpoint_work() {
        let (config, _) = fixture();
        let pin =
            |value| CodePin::new(Address::repeat_byte(value), B256::repeat_byte(value)).unwrap();
        let reader = ChainReader::new(
            Arc::new(PendingRpc),
            DeploymentProfile::new(
                Domain::new(5042002, Address::repeat_byte(3)).unwrap(),
                Address::repeat_byte(5),
                pin(3),
                pin(4),
                pin(5),
                pin(6),
            )
            .unwrap(),
        );
        let policy = VerifyPolicy::new(5, None).unwrap();
        assert_eq!(
            verify_endpoint(&config, &reader, &policy)
                .await
                .unwrap_err(),
            VerificationError::Deadline
        );
    }
}
