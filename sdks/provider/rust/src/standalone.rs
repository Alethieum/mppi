//! Optional owned listener for a provider application supplying an existing Mppi.
//!
//! This composes the SDK services and three discovery routes. The caller still
//! supplies execution, usage, wallet and recovery integration. TLS credentials,
//! DNS and ingress configuration remain provider-owned.

use axum::{
    body::Body,
    http::{Request, Response, StatusCode},
    routing::get,
};
use futures_core::Stream;
use std::{
    fmt,
    future::Future,
    io::{self, Read},
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpListener,
    sync::{OwnedSemaphorePermit, Semaphore},
    task::{JoinHandle, JoinSet},
    time::Instant,
};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    },
};
use tokio_util::sync::CancellationToken;
use tonic::{
    service::Routes,
    transport::{Server, server::Connected},
};
use zeroize::Zeroizing;

use crate::{
    config::ListenerConfig,
    interfaces::{DiscoveryResponse, ProviderLifecycle},
    runtime::Mppi,
    types::ShutdownResult,
};

const MAX_CREDENTIAL_BYTES: usize = 1 << 20;

/// Explicit finite lifetime and transport limits for the owned listener.
#[derive(Clone, Debug)]
pub struct ServePolicy {
    startup_timeout: Duration,
    shutdown_timeout: Duration,
    handshake_timeout: Duration,
    max_connections: usize,
    max_handshakes: usize,
}
impl ServePolicy {
    pub fn new(
        startup_timeout: Duration,
        shutdown_timeout: Duration,
        handshake_timeout: Duration,
        max_connections: usize,
        max_handshakes: usize,
    ) -> Result<Self, ServeError> {
        if [startup_timeout, shutdown_timeout, handshake_timeout]
            .iter()
            .any(|value| value.is_zero() || *value > Duration::from_secs(3600))
            || !(1..=65_536).contains(&max_connections)
            || !(1..=max_connections.min(1024)).contains(&max_handshakes)
        {
            return Err(ServeError::new(ServeFailure::Policy));
        }
        Ok(Self {
            startup_timeout,
            shutdown_timeout,
            handshake_timeout,
            max_connections,
            max_handshakes,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServeFailure {
    Policy,
    Credentials,
    Bind,
    Startup,
    Listener,
    Shutdown,
}

/// Sanitized failure with the actual SDK shutdown evidence, when available.
#[derive(Clone, Debug)]
pub struct ServeError {
    failure: ServeFailure,
    shutdown: Option<ShutdownResult>,
}
impl ServeError {
    fn new(failure: ServeFailure) -> Self {
        Self {
            failure,
            shutdown: None,
        }
    }
    pub fn failure(&self) -> ServeFailure {
        self.failure
    }
    pub fn shutdown(&self) -> Option<&ShutdownResult> {
        self.shutdown.as_ref()
    }
}
impl fmt::Display for ServeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.failure {
            ServeFailure::Policy => "invalid standalone serving limits",
            ServeFailure::Credentials => "standalone TLS credentials unavailable or invalid",
            ServeFailure::Bind => "standalone listener could not bind",
            ServeFailure::Startup => "MPPI startup did not establish readiness",
            ServeFailure::Listener => "standalone listener ended unexpectedly",
            ServeFailure::Shutdown => "standalone shutdown could not finish within its deadline",
        })
    }
}
impl std::error::Error for ServeError {}

fn credential(path: &Path) -> Result<Zeroizing<Vec<u8>>, ServeError> {
    // Symlinked certificate paths are supported for provider-managed rotation.
    // Check regular files before opening to reject devices and named pipes.
    let metadata =
        std::fs::metadata(path).map_err(|_| ServeError::new(ServeFailure::Credentials))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_CREDENTIAL_BYTES as u64 {
        return Err(ServeError::new(ServeFailure::Credentials));
    }
    let file = std::fs::File::open(path).map_err(|_| ServeError::new(ServeFailure::Credentials))?;
    if !file
        .metadata()
        .map_err(|_| ServeError::new(ServeFailure::Credentials))?
        .is_file()
    {
        return Err(ServeError::new(ServeFailure::Credentials));
    }
    let mut bytes = Zeroizing::new(Vec::new());
    file.take((MAX_CREDENTIAL_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| ServeError::new(ServeFailure::Credentials))?;
    if bytes.is_empty() || bytes.len() > MAX_CREDENTIAL_BYTES {
        return Err(ServeError::new(ServeFailure::Credentials));
    }
    Ok(bytes)
}

fn tls(config: &ListenerConfig) -> Result<Option<TlsAcceptor>, ServeError> {
    if config.tls_mode() == "ingress" {
        return Ok(None);
    }
    let cert = credential(
        config
            .tls_cert()
            .ok_or_else(|| ServeError::new(ServeFailure::Credentials))?,
    )?;
    let key = credential(
        config
            .tls_key()
            .ok_or_else(|| ServeError::new(ServeFailure::Credentials))?,
    )?;
    tls_from_pem(&cert, &key).map(Some)
}
fn tls_from_pem(cert: &[u8], key: &[u8]) -> Result<TlsAcceptor, ServeError> {
    let failure = || ServeError::new(ServeFailure::Credentials);
    let certs = CertificateDer::pem_slice_iter(cert)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| failure())?;
    let key = PrivateKeyDer::from_pem_slice(key).map_err(|_| failure())?;
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| failure())?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|_| failure())?;
    // Tonic's built-in TLS advertises only h2. Discovery also requires normal
    // HTTPS GETs, so this listener explicitly negotiates both protocols.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn routes(runtime: &Mppi) -> Routes {
    let (chat, control) = runtime.grpc_services();
    let sdk = runtime.clone();
    document_routes(
        Routes::new(chat).add_service(control),
        Arc::new(move |path| sdk.discovery(path)),
    )
}
type Documents = Arc<dyn Fn(&str) -> Option<DiscoveryResponse> + Send + Sync>;
fn document_routes(routes: Routes, documents: Documents) -> Routes {
    let mut router = routes.into_axum_router();
    for path in [
        "/discover",
        "/.well-known/agent-registration.json",
        "/models",
    ] {
        let documents = documents.clone();
        router = router.route(
            path,
            get(move |request: Request<Body>| {
                let documents = documents.clone();
                async move {
                    if request.uri().query().is_some() {
                        return Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(Body::empty())
                            .expect("static response");
                    }
                    let Some(document) = documents(path) else {
                        return Response::builder()
                            .status(StatusCode::SERVICE_UNAVAILABLE)
                            .body(Body::empty())
                            .expect("static response");
                    };
                    let mut response = Response::builder().status(document.status);
                    for (name, value) in document.headers {
                        response = response.header(name, value);
                    }
                    response
                        .body(Body::from(document.body))
                        .unwrap_or_else(|_| {
                            Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body(Body::empty())
                                .expect("static response")
                        })
                }
            }),
        );
    }
    router.into()
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
type Closing = Pin<Box<dyn Future<Output = ()> + Send>>;
struct OwnedIo {
    io: Box<dyn Io>,
    closing: Closing,
    _permit: OwnedSemaphorePermit,
}
impl OwnedIo {
    fn new(io: impl Io + 'static, close: CancellationToken, permit: OwnedSemaphorePermit) -> Self {
        Self {
            io: Box::new(io),
            closing: Box::pin(close.cancelled_owned()),
            _permit: permit,
        }
    }
    fn closing(&mut self, cx: &mut Context<'_>) -> bool {
        self.closing.as_mut().poll(cx).is_ready()
    }
}
impl Connected for OwnedIo {
    type ConnectInfo = ();
    fn connect_info(&self) {}
}
impl AsyncRead for OwnedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.closing(cx) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut *self.io).poll_read(cx, buffer)
    }
}
impl AsyncWrite for OwnedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.closing(cx) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut *self.io).poll_write(cx, data)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.closing(cx) {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut *self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.closing(cx) {
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut *self.io).poll_shutdown(cx)
    }
}
type PermitFuture =
    Pin<Box<dyn Future<Output = Result<OwnedSemaphorePermit, tokio::sync::AcquireError>> + Send>>;
struct Incoming {
    listener: TcpListener,
    tls: Option<TlsAcceptor>,
    close: CancellationToken,
    connections: Arc<Semaphore>,
    permit_future: Option<PermitFuture>,
    permit: Option<OwnedSemaphorePermit>,
    handshakes: JoinSet<io::Result<OwnedIo>>,
    policy: ServePolicy,
    failed: bool,
}
impl Incoming {
    fn new(
        listener: TcpListener,
        tls: Option<TlsAcceptor>,
        close: CancellationToken,
        connections: Arc<Semaphore>,
        policy: &ServePolicy,
    ) -> Self {
        Self {
            listener,
            tls,
            close,
            connections,
            permit_future: None,
            permit: None,
            handshakes: JoinSet::new(),
            policy: policy.clone(),
            failed: false,
        }
    }
}
impl Stream for Incoming {
    type Item = io::Result<OwnedIo>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.failed {
            return Poll::Ready(None);
        }
        // Limit work in one poll, including malformed handshakes.
        for _ in 0..64 {
            match self.handshakes.poll_join_next(cx) {
                Poll::Ready(Some(Ok(Ok(io)))) => return Poll::Ready(Some(Ok(io))),
                Poll::Ready(Some(_)) => continue,
                _ => {}
            }
            if self.handshakes.len() >= self.policy.max_handshakes {
                return Poll::Pending;
            }
            if self.permit.is_none() {
                if self.permit_future.is_none() {
                    self.permit_future = Some(Box::pin(self.connections.clone().acquire_owned()));
                }
                match self
                    .permit_future
                    .as_mut()
                    .expect("permit future")
                    .as_mut()
                    .poll(cx)
                {
                    Poll::Ready(Ok(permit)) => {
                        self.permit = Some(permit);
                        self.permit_future = None;
                    }
                    Poll::Ready(Err(_)) => {
                        self.failed = true;
                        return Poll::Ready(None);
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }
            let socket = match self.listener.poll_accept(cx) {
                Poll::Ready(Ok((socket, _))) => socket,
                Poll::Ready(Err(error)) => {
                    self.failed = true;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Pending => return Poll::Pending,
            };
            if let Err(error) = socket.set_nodelay(true) {
                return Poll::Ready(Some(Err(error)));
            }
            let permit = self.permit.take().expect("reserved connection permit");
            let close = self.close.clone();
            if let Some(tls) = self.tls.clone() {
                let timeout = self.policy.handshake_timeout;
                self.handshakes.spawn(async move {
                    let stream = tokio::time::timeout(timeout, tls.accept(socket))
                        .await
                        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
                    Ok(OwnedIo::new(stream, close, permit))
                });
            } else {
                return Poll::Ready(Some(Ok(OwnedIo::new(socket, close, permit))));
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

struct Cleanup {
    runtime: Arc<Mppi>,
    timeout: Duration,
    close: CancellationToken,
    accept_stop: CancellationToken,
    server: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
    armed: bool,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        self.accept_stop.cancel();
        self.close.cancel();
        if let Some(server) = &self.server {
            server.abort();
        }
        if self.armed {
            let runtime = self.runtime.clone();
            let deadline = Instant::now() + self.timeout;
            // A dropped caller future cannot receive a shutdown report. Still
            // stop SDK grants and tasks; callers use the token and await serve
            // when they need the returned unresolved-operation evidence.
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    let _ = runtime.shutdown(deadline).await;
                });
            }
        }
    }
}

/// Start one owned listener and the supplied SDK, then await caller shutdown.
/// Validate credentials and bind before starting SDK resources. All post-start
/// exits attempt bounded SDK shutdown and preserve its report. To receive that
/// report, cancel `shutdown` and await this future rather than dropping it.
pub async fn serve(
    runtime: Arc<Mppi>,
    listener_config: &ListenerConfig,
    shutdown: CancellationToken,
    policy: &ServePolicy,
) -> Result<ShutdownResult, ServeError> {
    let tls = tls(listener_config)?;
    let listener = tokio::time::timeout(
        policy.startup_timeout,
        TcpListener::bind(listener_config.bind()),
    )
    .await
    .map_err(|_| ServeError::new(ServeFailure::Bind))?
    .map_err(|_| ServeError::new(ServeFailure::Bind))?;
    let close = CancellationToken::new();
    let accept_stop = CancellationToken::new();
    let connections = Arc::new(Semaphore::new(policy.max_connections));
    let mut cleanup = Cleanup {
        runtime: runtime.clone(),
        timeout: policy.shutdown_timeout,
        close: close.clone(),
        accept_stop: accept_stop.clone(),
        server: None,
        armed: true,
    };
    let startup = tokio::select! {
        _ = shutdown.cancelled() => None,
        result = tokio::time::timeout(policy.startup_timeout, runtime.start()) => result.ok().and_then(Result::ok),
    };
    let mut failure = if startup.is_some_and(|ready| ready.ready()) {
        None
    } else {
        Some(ServeFailure::Startup)
    };
    if failure.is_none() {
        let incoming = Incoming::new(listener, tls, close.clone(), connections.clone(), policy);
        let routes = routes(&runtime);
        cleanup.server = Some(tokio::spawn(async move {
            Server::builder()
                .accept_http1(true)
                .http2_max_header_list_size(16 << 10)
                .max_concurrent_streams(1024)
                .serve_with_incoming_shutdown(routes, incoming, accept_stop.cancelled())
                .await
        }));
        tokio::select! {
            _ = shutdown.cancelled() => {}
            _ = cleanup.server.as_mut().expect("started server") => { failure = Some(ServeFailure::Listener); cleanup.server = None; }
        }
    } else {
        drop(listener);
    }
    cleanup.accept_stop.cancel();
    let deadline = Instant::now() + policy.shutdown_timeout;
    let result = runtime.shutdown(deadline).await;
    // SDK final reports have had their opportunity. Close owned sockets,
    // including idle HTTP keepalives and unauthenticated Control streams.
    close.cancel();
    let drained = if let Some(mut server) = cleanup.server.take() {
        match tokio::time::timeout_at(deadline, &mut server).await {
            Ok(Ok(Ok(()))) => true,
            _ => {
                server.abort();
                false
            }
        }
    } else {
        true
    };
    let released = tokio::time::timeout_at(
        deadline,
        connections.acquire_many(policy.max_connections as u32),
    )
    .await
    .is_ok();
    cleanup.armed = false;
    match result {
        Ok(report) if failure.is_none() && drained && released => Ok(report),
        Ok(report) => Err(ServeError {
            failure: failure.unwrap_or(ServeFailure::Shutdown),
            shutdown: Some(report),
        }),
        Err(_) => Err(ServeError::new(ServeFailure::Shutdown)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{http::HttpTransport, proto};
    use std::{
        fs,
        path::PathBuf,
        process::{Command, Stdio},
        sync::atomic::{AtomicUsize, Ordering},
    };
    use tokio::{net::TcpStream, sync::mpsc};
    use tonic::{
        Request as GrpcRequest, Response as GrpcResponse, Status, Streaming,
        transport::{Certificate, ClientTlsConfig, Endpoint},
    };

    struct Credentials(PathBuf);
    impl Credentials {
        fn new() -> Self {
            let mut id = [0u8; 16];
            getrandom::fill(&mut id).unwrap();
            let directory = std::env::temp_dir().join(format!(
                "mppi-standalone-{}",
                alloy_primitives::hex::encode(id)
            ));
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder.create(&directory).unwrap();
            let this = Self(directory);
            let run = |args: &[&str]| {
                assert!(
                    Command::new("openssl")
                        .args(args)
                        .current_dir(&this.0)
                        .stdin(Stdio::null())
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                        .unwrap()
                        .success()
                );
            };
            run(&[
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                "ca.key",
                "-out",
                "ca.pem",
                "-days",
                "1",
                "-subj",
                "/CN=MPPI standalone fixture CA",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-addext",
                "keyUsage=critical,keyCertSign,cRLSign",
            ]);
            run(&[
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                "key.pem",
                "-out",
                "leaf.csr",
                "-subj",
                "/CN=localhost",
            ]);
            fs::write(this.0.join("leaf.ext"), "basicConstraints=critical,CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:localhost,IP:127.0.0.1\n").unwrap();
            run(&[
                "x509",
                "-req",
                "-in",
                "leaf.csr",
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-out",
                "cert.pem",
                "-days",
                "1",
                "-extfile",
                "leaf.ext",
            ]);
            this
        }
        fn config(&self) -> ListenerConfig {
            ListenerConfig::new(
                "127.0.0.1:8443".into(),
                "direct".into(),
                Some(self.0.join("cert.pem")),
                Some(self.0.join("key.pem")),
            )
            .unwrap()
        }
        fn cert(&self) -> Vec<u8> {
            fs::read(self.0.join("cert.pem")).unwrap()
        }
        fn ca(&self) -> Vec<u8> {
            fs::read(self.0.join("ca.pem")).unwrap()
        }
    }
    impl Drop for Credentials {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn policy() -> ServePolicy {
        ServePolicy::new(
            Duration::from_secs(2),
            Duration::from_secs(2),
            Duration::from_millis(100),
            8,
            2,
        )
        .unwrap()
    }

    #[test]
    fn limits_are_finite_and_explicit() {
        for (startup, shutdown, handshake, connections, handshakes) in [
            (0, 1, 1, 1, 1),
            (1, 0, 1, 1, 1),
            (1, 1, 0, 1, 1),
            (3601, 1, 1, 1, 1),
            (1, 1, 1, 0, 1),
            (1, 1, 1, 65537, 1),
            (1, 1, 1, 1, 2),
            (1, 1, 1, 1, 0),
        ] {
            assert_eq!(
                ServePolicy::new(
                    Duration::from_secs(startup),
                    Duration::from_secs(shutdown),
                    Duration::from_secs(handshake),
                    connections,
                    handshakes
                )
                .unwrap_err()
                .failure(),
                ServeFailure::Policy
            );
        }
        policy();
    }

    #[test]
    fn credentials_fail_before_listener_and_support_provider_symlinks() {
        let files = Credentials::new();
        assert!(tls(&files.config()).unwrap().is_some());
        assert!(tls_from_pem(b"invalid", b"secret-key").is_err());
        let other = Credentials::new();
        assert!(
            tls_from_pem(
                &files.cert(),
                &credential(&other.0.join("key.pem")).unwrap()
            )
            .is_err()
        );
        assert_eq!(
            credential(&files.0).unwrap_err().failure(),
            ServeFailure::Credentials
        );
        let big = files.0.join("oversized");
        fs::write(&big, vec![1; MAX_CREDENTIAL_BYTES + 1]).unwrap();
        assert_eq!(
            credential(&big).unwrap_err().failure(),
            ServeFailure::Credentials
        );
        #[cfg(unix)]
        {
            let linked = files.0.join("linked.pem");
            std::os::unix::fs::symlink(files.0.join("cert.pem"), &linked).unwrap();
            assert_eq!(&*credential(&linked).unwrap(), &files.cert());
        }
        let ingress =
            ListenerConfig::new("127.0.0.1:8443".into(), "ingress".into(), None, None).unwrap();
        assert!(tls(&ingress).unwrap().is_none());
    }

    #[derive(Clone)]
    struct Peer(Arc<AtomicUsize>);
    struct Replies(mpsc::Receiver<Result<proto::ControlMessage, Status>>);
    impl Stream for Replies {
        type Item = Result<proto::ControlMessage, Status>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.0.poll_recv(cx)
        }
    }
    #[tonic::async_trait]
    impl proto::control_server::Control for Peer {
        type SessionStream = Replies;
        async fn session(
            &self,
            request: GrpcRequest<Streaming<proto::ControlMessage>>,
        ) -> Result<GrpcResponse<Self::SessionStream>, Status> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let (send, receive) = mpsc::channel(1);
            let mut input = request.into_inner();
            tokio::spawn(async move {
                let first = proto::ControlMessage {
                    kind: Some(proto::control_message::Kind::ReconnectChallenge(
                        proto::ReconnectChallenge {
                            nonce: Some(vec![1; 32]),
                        },
                    )),
                };
                if send.send(Ok(first)).await.is_err() {
                    return;
                }
                while let Ok(Some(_)) = input.message().await {}
            });
            Ok(GrpcResponse::new(Replies(receive)))
        }
    }
    struct Requests(mpsc::Receiver<proto::ControlMessage>);
    impl Stream for Requests {
        type Item = proto::ControlMessage;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.0.poll_recv(cx)
        }
    }
    fn fixture_routes(peer: Peer) -> Routes {
        document_routes(
            Routes::new(proto::control_server::ControlServer::new(peer)),
            Arc::new(|path| {
                Some(DiscoveryResponse {
                    status: 200,
                    headers: vec![("content-type".into(), "application/json".into())],
                    body: serde_json::to_vec(&serde_json::json!({"path":path})).unwrap(),
                })
            }),
        )
    }
    struct Fixture {
        origin: String,
        close: CancellationToken,
        stop: CancellationToken,
        connections: Arc<Semaphore>,
        task: JoinHandle<Result<(), tonic::transport::Error>>,
        count: Arc<AtomicUsize>,
    }
    impl Fixture {
        async fn new(tls: Option<TlsAcceptor>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let origin = format!(
                "{}://{}",
                if tls.is_some() { "https" } else { "http" },
                listener.local_addr().unwrap()
            );
            let close = CancellationToken::new();
            let stop = CancellationToken::new();
            let connections = Arc::new(Semaphore::new(8));
            let incoming =
                Incoming::new(listener, tls, close.clone(), connections.clone(), &policy());
            let count = Arc::new(AtomicUsize::new(0));
            let routes = fixture_routes(Peer(count.clone()));
            let stopping = stop.clone();
            let task = tokio::spawn(async move {
                Server::builder()
                    .accept_http1(true)
                    .serve_with_incoming_shutdown(routes, incoming, stopping.cancelled())
                    .await
            });
            Self {
                origin,
                close,
                stop,
                connections,
                task,
                count,
            }
        }
        async fn finish(self) {
            self.stop.cancel();
            self.close.cancel();
            tokio::time::timeout(Duration::from_secs(2), self.task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let _released =
                tokio::time::timeout(Duration::from_secs(2), self.connections.acquire_many(8))
                    .await
                    .unwrap()
                    .unwrap();
        }
    }
    async fn open_control(
        endpoint: Endpoint,
    ) -> (
        mpsc::Sender<proto::ControlMessage>,
        Streaming<proto::ControlMessage>,
    ) {
        let channel = endpoint.connect().await.unwrap();
        let mut client = proto::control_client::ControlClient::new(channel);
        let (send, receive) = mpsc::channel(1);
        let mut request = GrpcRequest::new(Requests(receive));
        request.set_timeout(Duration::from_secs(3));
        let mut stream = client.session(request).await.unwrap().into_inner();
        assert!(matches!(
            stream.message().await.unwrap().unwrap().kind,
            Some(proto::control_message::Kind::ReconnectChallenge(_))
        ));
        (send, stream)
    }

    #[tokio::test]
    async fn direct_tls_serves_http1_discovery_and_native_h2_on_one_port() {
        let credentials = Credentials::new();
        let fixture = Fixture::new(tls(&credentials.config()).unwrap()).await;
        // Loopback only; generous so a loaded test host cannot time it out.
        let transport = HttpTransport::new(10_000, 1024, Some(&credentials.ca())).unwrap();
        for path in [
            "/discover",
            "/.well-known/agent-registration.json",
            "/models",
        ] {
            let result = transport
                .request(&format!("{}{path}", fixture.origin), None)
                .await
                .unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&result).unwrap()["path"],
                path
            );
        }
        for path in [
            "/v1/chat/completions",
            "/models?private=value",
            "/discover/",
            "/unknown",
        ] {
            assert!(
                transport
                    .request(&format!("{}{path}", fixture.origin), None)
                    .await
                    .is_err()
            );
        }
        let endpoint = Endpoint::from_shared(fixture.origin.clone())
            .unwrap()
            .tls_config(
                ClientTlsConfig::new().ca_certificate(Certificate::from_pem(credentials.ca())),
            )
            .unwrap();
        let (send, mut output) = open_control(endpoint).await;
        assert_eq!(fixture.count.load(Ordering::SeqCst), 1);
        fixture.close.cancel();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), output.message())
                .await
                .unwrap()
                .is_err()
        );
        drop(send);
        drop(output);
        fixture.finish().await;
    }

    #[tokio::test]
    async fn explicit_ingress_serves_plaintext_private_listener() {
        let fixture = Fixture::new(None).await;
        let transport = HttpTransport::new(1000, 1024, None).unwrap();
        assert!(
            transport
                .request(&format!("{}/discover", fixture.origin), None)
                .await
                .is_ok()
        );
        let (send, output) =
            open_control(Endpoint::from_shared(fixture.origin.clone()).unwrap()).await;
        drop(output);
        drop(send);
        fixture.finish().await;
    }

    #[tokio::test]
    async fn slow_handshakes_are_bounded_and_released_on_drop() {
        let credentials = Credentials::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connections = Arc::new(Semaphore::new(8));
        let mut incoming = Incoming::new(
            listener,
            tls(&credentials.config()).unwrap(),
            CancellationToken::new(),
            connections.clone(),
            &policy(),
        );
        let mut clients = Vec::new();
        for _ in 0..4 {
            clients.push(TcpStream::connect(address).await.unwrap());
        }
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                std::future::poll_fn(|cx| Pin::new(&mut incoming).poll_next(cx))
            )
            .await
            .is_err()
        );
        assert_eq!(incoming.handshakes.len(), 2);
        assert_eq!(connections.available_permits(), 6);
        drop(incoming);
        let _released = tokio::time::timeout(Duration::from_secs(1), connections.acquire_many(8))
            .await
            .unwrap()
            .unwrap();
        drop(clients);
    }
}
