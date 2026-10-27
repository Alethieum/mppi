//! Chain reads over a local kept-alive JSON-RPC server that counts HTTP round
//! trips and TCP connections. No wallet or live network is contacted.
use alloy_primitives::{Address, B256, Bytes, U256, address, hex, keccak256};
use alloy_sol_types::{SolCall, sol};
use mppi_provider::{
    chain::{ChainError, ChainReader, CodePin, DeploymentProfile, Rpc},
    discovery::MAX_DOCUMENT_BYTES,
    follower::ChainFollower,
    http::{HttpJsonRpc, HttpTransport, MAX_RPC_RESPONSE_BYTES, TransportError},
    signing::Voucher,
    types::Domain,
};
use serde_json::{Value, json};
use std::{
    collections::{HashSet, VecDeque},
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const CHAIN: u64 = 5042002;
const PROVIDER: u64 = 7;
const AGENT_URI: &str = "https://Provider.EXAMPLE:443/discover";
const CATALOG: &[u8] = br#"[{"cache":"0","input":"123","model":"example/model","output":"456"}]"#;
// Multicall3's runtime code as deployed at its canonical address (Ethereum
// mainnet and Arc testnet explorers, read October 2026; MIT-licensed source at
// github.com/mds1/multicall3). Its Keccak-256 is the hash the reader pins.
const MULTICALL3_CODE: &str = include_str!("multicall3_runtime.hex");
const MULTICALL3: Address = address!("ca11bde05977b3631167028862be2a173976ca11");

sol! {
    interface Views {
        function getBlockNumber() external view returns (uint256);
        function getCurrentBlockTimestamp() external view returns (uint256);
        function getLastBlockHash() external view returns (bytes32);
        function voucherDigest(bytes32 sessionId, uint64 cumulativeAmount) external view returns (bytes32);
        function pauseLevel() external view returns (uint8);
        function getSession(bytes32 sessionId) external view returns (uint64, uint64, uint8, uint64, uint64, bytes32);
        function ownerOf(uint256 agentId) external view returns (address);
        function getAgentWallet(uint256 agentId) external view returns (address);
        function tokenURI(uint256 agentId) external view returns (string);
        function getMetadata(uint256 agentId, string key) external view returns (bytes);
    }
    interface Aggregate {
        struct Call3 { address target; bool allowFailure; bytes callData; }
        struct Answer { bool success; bytes returnData; }
        function aggregate3(Call3[] calls) external payable returns (Answer[]);
    }
}

fn proxy() -> Address {
    Address::repeat_byte(0x99)
}
fn registry() -> Address {
    Address::repeat_byte(0x97)
}
fn session_id() -> B256 {
    B256::repeat_byte(0x55)
}
fn hx(raw: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(raw))
}
fn domain() -> Domain {
    Domain::new(CHAIN, proxy()).unwrap()
}
fn profile() -> DeploymentProfile {
    let pin = |byte| CodePin::new(Address::repeat_byte(byte), keccak256([0x60, 0])).unwrap();
    DeploymentProfile::new(
        domain(),
        registry(),
        pin(0x99),
        pin(0x98),
        pin(0x97),
        pin(0x96),
    )
    .unwrap()
}
fn block_hash(number: u64) -> B256 {
    keccak256(number.to_be_bytes())
}

/// One Arc-like chain, served by a backend whose head may lag the chain.
struct Chain {
    multicall: bool,
    batches: bool,
    head: AtomicU64,
    start: u64,
    statuses: Mutex<VecDeque<u16>>,
    // The Retry-After field sent with each refused status, if any.
    retry_after: Mutex<VecDeque<&'static str>>,
    errors: Mutex<VecDeque<Value>>,
    // Blocks whose read by number is refused once with QuickNode's -32007,
    // as a per-second limit refuses some entries of a batch.
    limited: Mutex<HashSet<u64>>,
    delays: Mutex<VecDeque<Duration>>,
    // The head moves on by one block once this many more aggregate3 calls at
    // `latest` have been answered, as between two backends' answers.
    advance_after: Mutex<Option<usize>>,
    // Each aggregate3 answered: the block it ran at, its view count, its
    // registry views and whether it was pinned by block hash.
    aggregates: Mutex<Vec<(u64, usize, usize, bool)>>,
    // Arc's -32014 for every state read pinned by block hash: the backend
    // serving it "hasn't yet imported that block".
    unimported: AtomicBool,
}
impl Chain {
    fn new(multicall: bool) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        Self {
            multicall,
            batches: true,
            head: AtomicU64::new(100),
            // Block 100 is ten seconds old; later blocks stay in the past.
            start: now - 110,
            statuses: Mutex::new(VecDeque::new()),
            retry_after: Mutex::new(VecDeque::new()),
            errors: Mutex::new(VecDeque::new()),
            limited: Mutex::new(HashSet::new()),
            delays: Mutex::new(VecDeque::new()),
            advance_after: Mutex::new(None),
            aggregates: Mutex::new(Vec::new()),
            unimported: AtomicBool::new(false),
        }
    }
    fn header(&self, number: u64) -> Value {
        if number > self.head.load(Ordering::SeqCst) {
            return Value::Null;
        }
        json!({"number":format!("0x{number:x}"),"hash":hx(block_hash(number)),"parentHash":hx(block_hash(number - 1)),"timestamp":format!("0x{:x}", self.start + number),"gasLimit":"0x1c9c380"})
    }
    /// A view's return data at block `head`.
    fn view(&self, to: Address, data: &[u8], head: u64) -> Vec<u8> {
        let selector: [u8; 4] = data[..4].try_into().unwrap();
        let word = |value: u64| U256::from(value).to_be_bytes::<32>().to_vec();
        let is = |target: Address, wanted: [u8; 4]| to == target && selector == wanted;
        if is(MULTICALL3, Views::getBlockNumberCall::SELECTOR) {
            word(head)
        } else if is(MULTICALL3, Views::getCurrentBlockTimestampCall::SELECTOR) {
            word(self.start + head)
        } else if is(MULTICALL3, Views::getLastBlockHashCall::SELECTOR) {
            block_hash(head - 1).to_vec()
        } else if is(proxy(), Views::voucherDigestCall::SELECTOR) {
            domain()
                .signing_context()
                .digest(&Voucher {
                    sessionId: B256::ZERO,
                    cumulativeAmount: 0,
                })
                .to_vec()
        } else if is(proxy(), Views::pauseLevelCall::SELECTOR) {
            word(0)
        } else if is(proxy(), Views::getSessionCall::SELECTOR) {
            // Every session holds the same deposit; its last activity names
            // the block it was read at.
            [
                word(6_000_000),
                word(0),
                word(0),
                word(0),
                word(self.start + head),
                vec![0x66; 32],
            ]
            .concat()
        } else if is(registry(), Views::ownerOfCall::SELECTOR) {
            Address::repeat_byte(0x11).into_word().to_vec()
        } else if is(registry(), Views::getAgentWalletCall::SELECTOR) {
            Address::repeat_byte(0x22).into_word().to_vec()
        } else if is(registry(), Views::tokenURICall::SELECTOR) {
            Views::tokenURICall::abi_encode_returns(&AGENT_URI.to_owned())
        } else if is(registry(), Views::getMetadataCall::SELECTOR) {
            Views::getMetadataCall::abi_encode_returns(&Bytes::from(CATALOG.to_vec()))
        } else {
            panic!("unplanned view {} {}", hx(to), hx(data))
        }
    }
    fn answer(&self, request: &Value) -> Value {
        let id = request["id"].clone();
        if let Some(error) = self.errors.lock().unwrap().pop_front() {
            return json!({"jsonrpc":"2.0","id":id,"error":error});
        }
        let params = &request["params"];
        let quantity = |value: &Value| {
            u64::from_str_radix(value.as_str().unwrap().trim_start_matches("0x"), 16).unwrap()
        };
        let address = |value: &Value| {
            Address::from_slice(&hex::decode(&value.as_str().unwrap()[2..]).unwrap())
        };
        let result = match request["method"].as_str().unwrap() {
            "eth_chainId" => json!(format!("0x{CHAIN:x}")),
            "eth_getBlockByNumber" => {
                let number = if params[0] == "latest" {
                    self.head.load(Ordering::SeqCst)
                } else {
                    quantity(&params[0])
                };
                if self.limited.lock().unwrap().remove(&number) {
                    return json!({"jsonrpc":"2.0","id":id,"error":{"code":-32007}});
                }
                self.header(number)
            }
            "eth_getCode" => {
                let at = address(&params[0]);
                json!(if at == MULTICALL3 {
                    if self.multicall {
                        MULTICALL3_CODE.trim()
                    } else {
                        "0x"
                    }
                } else if [0x99, 0x98, 0x97, 0x96]
                    .map(Address::repeat_byte)
                    .contains(&at)
                {
                    "0x6000"
                } else {
                    "0x"
                })
            }
            "eth_getStorageAt" => {
                let at = address(&params[0]);
                json!(hx(Address::repeat_byte(if at == proxy() {
                    0x98
                } else {
                    0x96
                })
                .into_word()))
            }
            "eth_call" => {
                let to = address(&params[0]["to"]);
                let data = hex::decode(&params[0]["data"].as_str().unwrap()[2..]).unwrap();
                let head = self.head.load(Ordering::SeqCst);
                let (at, pinned) = if params[1] == "latest" {
                    (head, false)
                } else {
                    assert_eq!(params[1]["requireCanonical"], json!(true));
                    if self.unimported.load(Ordering::SeqCst) {
                        return json!({"jsonrpc":"2.0","id":id,"error":{"code":-32014,"message":"not yet imported"}});
                    }
                    let at = (0..=head)
                        .find(|n| params[1]["blockHash"] == hx(block_hash(*n)))
                        .expect("a pinned read names a served block");
                    (at, true)
                };
                if to == MULTICALL3 && self.multicall {
                    let calls = Aggregate::aggregate3Call::abi_decode(&data).unwrap().calls;
                    let answers = calls
                        .iter()
                        .map(|call| {
                            assert!(!call.allowFailure);
                            Aggregate::Answer {
                                success: true,
                                returnData: self.view(call.target, &call.callData, at).into(),
                            }
                        })
                        .collect::<Vec<_>>();
                    let registry = calls.iter().filter(|c| c.target == registry()).count();
                    let mut aggregates = self.aggregates.lock().unwrap();
                    aggregates.push((at, calls.len(), registry, pinned));
                    let mut advance = self.advance_after.lock().unwrap();
                    if let Some(left) = advance.as_mut().filter(|_| !pinned) {
                        *left -= 1;
                        if *left == 0 {
                            *advance = None;
                            self.head.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    json!(hx(Aggregate::aggregate3Call::abi_encode_returns(&answers)))
                } else if to == MULTICALL3 {
                    json!("0x")
                } else {
                    json!(hx(self.view(to, &data, at)))
                }
            }
            "eth_getTransactionReceipt" => Value::Null,
            method => panic!("unexpected RPC method {method}"),
        };
        json!({"jsonrpc":"2.0","id":id,"result":result})
    }
}

struct Server {
    url: String,
    chain: Arc<Chain>,
    requests: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    bodies: Arc<Mutex<Vec<Value>>>,
    // Each request body's length in bytes.
    sizes: Arc<Mutex<Vec<usize>>>,
}
impl Server {
    fn start(chain: Chain) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!(
            "http://{}/rpc/secret-key?token=secret",
            listener.local_addr().unwrap()
        );
        let server = Self {
            url,
            chain: Arc::new(chain),
            requests: Arc::new(AtomicUsize::new(0)),
            connections: Arc::new(AtomicUsize::new(0)),
            bodies: Arc::new(Mutex::new(Vec::new())),
            sizes: Arc::new(Mutex::new(Vec::new())),
        };
        let (chain, requests, connections, bodies, sizes) = (
            server.chain.clone(),
            server.requests.clone(),
            server.connections.clone(),
            server.bodies.clone(),
            server.sizes.clone(),
        );
        thread::spawn(move || {
            for socket in listener.incoming() {
                let Ok(socket) = socket else { return };
                connections.fetch_add(1, Ordering::SeqCst);
                let (chain, requests, bodies, sizes) = (
                    chain.clone(),
                    requests.clone(),
                    bodies.clone(),
                    sizes.clone(),
                );
                thread::spawn(move || serve(socket, &chain, &requests, &bodies, &sizes));
            }
        });
        server
    }
    fn rpc(&self, timeout_ms: u64) -> HttpJsonRpc {
        HttpJsonRpc::new(
            self.url.clone(),
            HttpTransport::new(timeout_ms, MAX_RPC_RESPONSE_BYTES, None).unwrap(),
        )
        .unwrap()
    }
    fn requests(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}
fn serve(
    socket: TcpStream,
    chain: &Chain,
    requests: &AtomicUsize,
    bodies: &Mutex<Vec<Value>>,
    sizes: &Mutex<Vec<usize>>,
) {
    let mut reader = BufReader::new(socket.try_clone().unwrap());
    let mut socket = socket;
    loop {
        let mut length = 0;
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return;
        }
        loop {
            line.clear();
            reader.read_line(&mut line).unwrap();
            if line == "\r\n" {
                break;
            }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = value.trim().parse().unwrap();
            }
        }
        let mut body = vec![0; length];
        reader.read_exact(&mut body).unwrap();
        sizes.lock().unwrap().push(length);
        requests.fetch_add(1, Ordering::SeqCst);
        let request: Value = serde_json::from_slice(&body).unwrap();
        bodies.lock().unwrap().push(request.clone());
        if let Some(delay) = chain.delays.lock().unwrap().pop_front() {
            thread::sleep(delay);
        }
        let status = chain.statuses.lock().unwrap().pop_front().unwrap_or(200);
        let response = match &request {
            _ if status != 200 => json!({}),
            Value::Array(_) if !chain.batches => {
                json!({"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"no batches"}})
            }
            Value::Array(entries) => {
                // Responses may arrive in any order (JSON-RPC 2.0 section 6).
                Value::Array(
                    entries
                        .iter()
                        .rev()
                        .map(|entry| chain.answer(entry))
                        .collect(),
                )
            }
            single => chain.answer(single),
        };
        let body = serde_json::to_vec(&response).unwrap();
        let retry_after = match chain.retry_after.lock().unwrap().pop_front() {
            Some(value) if status != 200 => format!("Retry-After: {value}\r\n"),
            _ => String::new(),
        };
        let head = format!(
            "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{retry_after}\r\n",
            body.len()
        );
        if socket.write_all(head.as_bytes()).is_err() || socket.write_all(&body).is_err() {
            return;
        }
    }
}

async fn follower(server: &Server) -> ChainFollower {
    let reader = ChainReader::new(Arc::new(server.rpc(5_000)), profile());
    let follower = ChainFollower::new(reader, 60_000, 1_000, Some(60_000)).unwrap();
    follower.start().await.unwrap();
    follower.watch_provider(U256::from(PROVIDER)).await.unwrap();
    follower.watch_session(session_id()).await.unwrap();
    follower
}

#[tokio::test]
async fn a_refresh_is_one_round_trip_on_one_kept_alive_connection() {
    let server = Server::start(Chain::new(true));
    let follower = follower(&server).await;
    let before = server.requests();
    for _ in 0..3 {
        server.chain.head.fetch_add(1, Ordering::SeqCst);
        let snapshot = follower.refresh().await.unwrap();
        assert_eq!(
            snapshot.block().observation().block_number(),
            server.chain.head.load(Ordering::SeqCst)
        );
        assert!(
            snapshot.identities()[&U256::from(PROVIDER)]
                .catalog()
                .is_some()
        );
        assert_eq!(snapshot.sessions()[&session_id()].deposit(), 6_000_000);
    }
    let per_refresh = (server.requests() - before) as f64 / 3.0;
    let connections = server.connections.load(Ordering::SeqCst);
    assert_eq!(
        (per_refresh, connections),
        (1.0, 1),
        "round trips per refresh and TCP connections"
    );
    follower.stop().await.unwrap();
}

#[tokio::test]
async fn without_verified_multicall3_a_refresh_is_two_round_trips() {
    let server = Server::start(Chain::new(false));
    let follower = follower(&server).await;
    let before = server.requests();
    for _ in 0..3 {
        server.chain.head.fetch_add(1, Ordering::SeqCst);
        follower.refresh().await.unwrap();
    }
    let per_refresh = (server.requests() - before) as f64 / 3.0;
    let connections = server.connections.load(Ordering::SeqCst);
    assert_eq!(
        (per_refresh, connections),
        (2.0, 1),
        "round trips per refresh and TCP connections"
    );
    follower.stop().await.unwrap();
}

#[tokio::test]
async fn transient_reads_are_repeated_at_most_twice() {
    for statuses in [vec![503], vec![429, 502], vec![408, 425]] {
        let server = Server::start(Chain::new(true));
        *server.chain.statuses.lock().unwrap() = statuses.clone().into();
        let result = server.rpc(1_000).call("eth_chainId", json!([])).await;
        assert_eq!(result.unwrap(), json!("0x4cef52"), "{statuses:?}");
        assert_eq!(server.requests(), statuses.len() + 1);
    }
    for code in [-32005, -32007, -32014] {
        let server = Server::start(Chain::new(true));
        server
            .chain
            .errors
            .lock()
            .unwrap()
            .push_back(json!({"code":code,"message":"slow down"}));
        assert!(
            server
                .rpc(1_000)
                .call("eth_chainId", json!([]))
                .await
                .is_ok()
        );
        assert_eq!(server.requests(), 2, "{code}");
    }
    let server = Server::start(Chain::new(true));
    *server.chain.statuses.lock().unwrap() = vec![503, 503, 503, 503].into();
    assert_eq!(
        server.rpc(1_000).call("eth_chainId", json!([])).await,
        Err(ChainError::Transport(TransportError::Status(503)))
    );
    assert_eq!(server.requests(), 3);
}

#[tokio::test]
async fn a_timed_out_read_is_repeated_once_within_its_budget() {
    let server = Server::start(Chain::new(true));
    *server.chain.delays.lock().unwrap() = vec![Duration::from_millis(400)].into();
    let result = server.rpc(200).call("eth_chainId", json!([])).await;
    assert_eq!(result.unwrap(), json!("0x4cef52"));
    assert_eq!(server.requests(), 2);
}

#[tokio::test]
async fn definite_answers_and_sends_are_never_repeated() {
    for (method, error, expected) in [
        (
            "eth_call",
            json!({"code":3,"message":"execution reverted: secret","data":"0x8baa579f00"}),
            ChainError::Reverted(Some([0x8b, 0xaa, 0x57, 0x9f])),
        ),
        (
            "eth_estimateGas",
            json!({"code":3,"message":"execution reverted"}),
            ChainError::Reverted(None),
        ),
        (
            "eth_call",
            json!({"code":-32000,"message":"header not found"}),
            ChainError::Rpc(-32000),
        ),
        (
            "eth_getCode",
            json!({"code":3,"message":"not an execution"}),
            ChainError::Rpc(3),
        ),
        (
            "eth_call",
            json!({"code":-32602,"message":"invalid params"}),
            ChainError::Rpc(-32602),
        ),
    ] {
        let server = Server::start(Chain::new(true));
        server.chain.errors.lock().unwrap().push_back(error.clone());
        let rpc = server.rpc(1_000);
        let failure = rpc
            .call(method, json!([{"to":hx(proxy()),"data":"0x"}, "latest"]))
            .await
            .unwrap_err();
        assert_eq!(server.requests(), 1, "{error}");
        assert_eq!(failure, expected);
        let text = format!("{failure} {failure:?}");
        assert!(
            !text.contains("secret") && !text.contains("header"),
            "{text}"
        );
    }
    // One reverted read in a batch fails the batch without repeating it.
    let server = Server::start(Chain::new(true));
    server
        .chain
        .errors
        .lock()
        .unwrap()
        .push_back(json!({"code":3,"message":"execution reverted"}));
    let calls = [
        ("eth_chainId", json!([])),
        (
            "eth_call",
            json!([{"to":hx(proxy()),"data":"0x"}, "latest"]),
        ),
    ];
    assert!(matches!(
        server.rpc(1_000).call_many(&calls).await,
        Err(ChainError::Reverted(None))
    ));
    assert_eq!(server.requests(), 1);
    let server = Server::start(Chain::new(true));
    let rpc = server.rpc(1_000);
    for method in ["eth_sendRawTransaction", "eth_sendTransaction", "eth_sign"] {
        assert!(matches!(
            rpc.call(method, json!(["0x00"])).await,
            Err(ChainError::InvalidEvidence(_))
        ));
        let calls = [("eth_chainId", json!([])), (method, json!(["0x00"]))];
        assert!(rpc.call_many(&calls).await.is_err());
    }
    assert_eq!(server.requests(), 0);
}

#[tokio::test]
async fn an_endpoint_without_batches_is_read_one_call_at_a_time() {
    let mut chain = Chain::new(true);
    chain.batches = false;
    let server = Server::start(chain);
    let rpc = server.rpc(1_000);
    let calls = [
        ("eth_chainId", json!([])),
        ("eth_getBlockByNumber", json!(["latest", false])),
    ];
    let results = rpc.call_many(&calls).await.unwrap();
    assert_eq!(results[0], json!("0x4cef52"));
    assert_eq!(results[1]["number"], json!("0x64"));
    // The refused batch, then each call; later reads skip the batch.
    assert_eq!(server.requests(), 3);
    rpc.call_many(&calls).await.unwrap();
    assert_eq!(server.requests(), 5);
}

#[tokio::test]
async fn failures_never_reveal_the_endpoint() {
    let server = Server::start(Chain::new(true));
    *server.chain.statuses.lock().unwrap() = vec![401, 401, 401].into();
    let failure = server
        .rpc(1_000)
        .call("eth_chainId", json!([]))
        .await
        .unwrap_err();
    assert_eq!(failure, ChainError::Transport(TransportError::Status(401)));
    let text = format!("{failure} {failure:?}");
    assert!(
        !text.contains("secret") && !text.contains("127.0.0.1"),
        "{text}"
    );
    assert_eq!(server.requests(), 1);
}

fn blocks(numbers: &[u64]) -> Vec<(&'static str, Value)> {
    numbers
        .iter()
        .map(|n| ("eth_getBlockByNumber", json!([format!("0x{n:x}"), false])))
        .collect()
}
/// The block each call of a request body reads, in order: a list for a batch.
fn sent(body: &Value) -> Value {
    match body {
        Value::Array(calls) => calls.iter().map(|call| call["params"][0].clone()).collect(),
        call => call["params"][0].clone(),
    }
}

#[tokio::test]
async fn only_the_refused_entries_of_a_batch_are_sent_again() {
    // JSON-RPC 2.0 section 6 matches each answer to its call by id, so the
    // entries a batch answered are kept and only the refused ones repeat.
    let server = Server::start(Chain::new(true));
    server.chain.limited.lock().unwrap().extend([1, 3, 6]);
    let rpc = server.rpc(1_000);
    let results = rpc.call_many(&blocks(&[1, 2, 3, 4])).await.unwrap();
    let numbers: Vec<_> = results
        .iter()
        .map(|block| block["number"].clone())
        .collect();
    assert_eq!(
        numbers,
        [json!("0x1"), json!("0x2"), json!("0x3"), json!("0x4")]
    );
    let results = rpc.call_many(&blocks(&[5, 6])).await.unwrap();
    assert_eq!(results[1]["number"], json!("0x6"));
    let bodies: Vec<_> = server.bodies.lock().unwrap().iter().map(sent).collect();
    assert_eq!(
        bodies,
        [
            json!(["0x1", "0x2", "0x3", "0x4"]),
            json!(["0x1", "0x3"]),
            json!(["0x5", "0x6"]),
            json!("0x6"), // One refused entry is sent again as a single call.
        ]
    );
}

#[tokio::test]
async fn one_call_at_a_time_a_refused_call_repeats_without_those_answered() {
    let mut chain = Chain::new(true);
    chain.batches = false;
    let server = Server::start(chain);
    server.chain.limited.lock().unwrap().insert(2);
    let results = server
        .rpc(1_000)
        .call_many(&blocks(&[1, 2, 3]))
        .await
        .unwrap();
    assert_eq!(results[2]["number"], json!("0x3"));
    let bodies: Vec<_> = server.bodies.lock().unwrap().iter().map(sent).collect();
    assert_eq!(
        bodies,
        [
            json!(["0x1", "0x2", "0x3"]), // Refused as a batch: then one call at a time.
            json!("0x1"),
            json!("0x2"),
            json!("0x2"),
            json!("0x3"),
        ]
    );
}

#[tokio::test]
async fn a_definite_answer_in_a_batch_ends_the_read_even_beside_a_refusal() {
    let server = Server::start(Chain::new(true));
    server.chain.limited.lock().unwrap().insert(1);
    // The server answers a batch from its last entry; this error goes to 0x2.
    server
        .chain
        .errors
        .lock()
        .unwrap()
        .push_back(json!({"code":-32602,"message":"invalid params"}));
    let failure = server.rpc(1_000).call_many(&blocks(&[1, 2])).await;
    assert_eq!(failure, Err(ChainError::Rpc(-32602)));
    assert_eq!(server.requests(), 1);
}

#[tokio::test]
async fn a_read_waits_out_retry_after_before_repeating() {
    let server = Server::start(Chain::new(true));
    *server.chain.statuses.lock().unwrap() = vec![429].into();
    *server.chain.retry_after.lock().unwrap() = vec!["1"].into();
    let started = Instant::now();
    let result = server.rpc(1_000).call("eth_chainId", json!([])).await;
    assert_eq!(result.unwrap(), json!("0x4cef52"));
    assert_eq!(server.requests(), 2);
    assert!(started.elapsed() >= Duration::from_secs(1));
}

#[tokio::test]
async fn a_retry_after_beyond_the_read_budget_fails_the_read_at_once() {
    // Repeats start within one 1 s attempt timeout plus 1.25 s; 30 s cannot fit.
    for value in ["30", "Fri, 31 Dec 9999 23:59:59 GMT"] {
        let server = Server::start(Chain::new(true));
        *server.chain.statuses.lock().unwrap() = vec![503].into();
        *server.chain.retry_after.lock().unwrap() = vec![value].into();
        let started = Instant::now();
        let failure = server.rpc(1_000).call("eth_chainId", json!([])).await;
        assert_eq!(
            failure,
            Err(ChainError::Transport(TransportError::Status(503)))
        );
        assert_eq!(server.requests(), 1, "{value}");
        assert!(started.elapsed() < Duration::from_millis(900), "{value}");
    }
}

/// Watches `count` more sessions, read by one refresh: while a refresh holds
/// the follower's lock, a watch records its id and then waits for the lock.
async fn watch_many(server: &Server, follower: &ChainFollower, count: u64) -> Vec<B256> {
    let ids: Vec<B256> = (0..count).map(|i| B256::from(U256::from(i + 1))).collect();
    server
        .chain
        .delays
        .lock()
        .unwrap()
        .push_back(Duration::from_millis(200));
    let refresh = follower.refresh();
    tokio::pin!(refresh);
    // Polled once, the refresh takes the lock and waits for the delayed answer.
    tokio::select! {
        biased;
        _ = &mut refresh => panic!("a delayed refresh answered at once"),
        () = std::future::ready(()) => {}
    }
    for id in &ids {
        tokio::select! {
            biased;
            _ = follower.watch_session(*id) => panic!("a watch refreshed while the lock was held"),
            () = std::future::ready(()) => {}
        }
    }
    refresh.await.unwrap();
    ids
}
/// The aggregate3 calls answered from `from` on.
fn aggregates(server: &Server, from: usize) -> Vec<(u64, usize, usize, bool)> {
    server.chain.aggregates.lock().unwrap()[from..].to_vec()
}

#[tokio::test]
async fn a_refresh_of_2500_sessions_reads_every_one_at_one_block_in_bounded_requests() {
    let server = Server::start(Chain::new(true));
    let follower = follower(&server).await;
    let ids = watch_many(&server, &follower, 2_500).await;
    let (requests, answered) = (
        server.requests(),
        server.chain.aggregates.lock().unwrap().len(),
    );
    server.chain.head.fetch_add(1, Ordering::SeqCst);
    let snapshot = follower.refresh().await.unwrap();
    let number = snapshot.block().observation().block_number();
    assert_eq!(number, server.chain.head.load(Ordering::SeqCst));
    assert_eq!(snapshot.sessions().len(), 2_501);
    for id in ids.iter().chain([&session_id()]) {
        // Each session's last activity names the block it was read at.
        let session = &snapshot.sessions()[id];
        assert_eq!(session.deposit(), 6_000_000);
        assert_eq!(session.last_activity(), server.chain.start + number, "{id}");
    }
    assert!(follower.readiness(Some(ids[2_499])).ready());
    // Each aggregate3 ran at the snapshot's block, within its gas bound: 512
    // views besides its three head views, and one provider's four identity views.
    let calls = aggregates(&server, answered);
    assert_eq!(calls.len(), 5, "{calls:?}");
    assert!(
        calls
            .iter()
            .all(|&(at, views, registry, pinned)| at == number
                && views <= 3 + 512
                && registry <= 4
                && !pinned),
        "{calls:?}"
    );
    // About 1.1 MB of requests: two exchanges, each body within the transport's bound.
    assert_eq!(server.requests() - requests, 2);
    let sizes = server.sizes.lock().unwrap().clone();
    assert!(
        sizes.iter().all(|&size| size <= MAX_DOCUMENT_BYTES),
        "{sizes:?}"
    );
    follower.stop().await.unwrap();
}

#[tokio::test]
async fn chunks_that_ran_at_another_head_are_read_again_pinned_to_the_header() {
    let server = Server::start(Chain::new(true));
    let follower = follower(&server).await;
    watch_many(&server, &follower, 1_100).await;
    let (requests, answered) = (
        server.requests(),
        server.chain.aggregates.lock().unwrap().len(),
    );
    let before = server.chain.head.load(Ordering::SeqCst);
    // The server answers a batch from its last entry: the last chunk runs at
    // this head, then the head moves on before the other chunks and the header.
    *server.chain.advance_after.lock().unwrap() = Some(1);
    let snapshot = follower.refresh().await.unwrap();
    let number = snapshot.block().observation().block_number();
    assert_eq!(number, before + 1);
    assert!(
        snapshot
            .sessions()
            .values()
            .all(|s| s.last_activity() == server.chain.start + number),
        "every session at the header's block"
    );
    let calls = aggregates(&server, answered);
    assert_eq!(
        calls.iter().map(|c| (c.0, c.3)).collect::<Vec<_>>(),
        [
            (before, false),
            (number, false),
            (number, false),
            (number, true)
        ],
        "the chunk that ran elsewhere was read again, pinned"
    );
    assert_eq!(server.requests() - requests, 2);
    let again = server.bodies.lock().unwrap().last().unwrap().clone();
    let pinned = json!({"blockHash":hx(block_hash(number)),"requireCanonical":true});
    assert_eq!(again.as_array().unwrap().len(), 2);
    assert!(again.as_array().unwrap().iter().any(|call| {
        call["method"] == "eth_call"
            && call["params"][0]["to"] == hx(MULTICALL3)
            && call["params"][1] == pinned
    }));
    assert!(again.as_array().unwrap().iter().any(|call| {
        call["method"] == "eth_getBlockByNumber" && call["params"][0] == format!("0x{number:x}")
    }));
    follower.stop().await.unwrap();
}

#[tokio::test]
async fn the_common_refresh_is_one_round_trip_even_over_several_chunks() {
    let server = Server::start(Chain::new(true));
    let follower = follower(&server).await;
    watch_many(&server, &follower, 1_000).await;
    let (requests, answered) = (
        server.requests(),
        server.chain.aggregates.lock().unwrap().len(),
    );
    server.chain.head.fetch_add(1, Ordering::SeqCst);
    let snapshot = follower.refresh().await.unwrap();
    assert_eq!(snapshot.sessions().len(), 1_001);
    assert_eq!(server.requests() - requests, 1);
    assert_eq!(aggregates(&server, answered).len(), 2);
    follower.stop().await.unwrap();
}

/// An aggregate3 `eth_call` at `latest` of `sessions` getSession views.
fn aggregate_call(sessions: u64) -> (&'static str, Value) {
    let calls = (0..sessions)
        .map(|i| Aggregate::Call3 {
            target: proxy(),
            allowFailure: false,
            callData: Views::getSessionCall {
                sessionId: B256::from(U256::from(i)),
            }
            .abi_encode()
            .into(),
        })
        .collect();
    let data = Aggregate::aggregate3Call { calls }.abi_encode();
    (
        "eth_call",
        json!([{"to":hx(MULTICALL3),"data":hx(data)}, "latest"]),
    )
}

#[tokio::test]
async fn no_request_body_exceeds_the_transport_bound() {
    let server = Server::start(Chain::new(true));
    let rpc = server.rpc(5_000);
    // Twelve calls of about 99 KB each: two batches, never one over 1 MiB.
    let calls = vec![aggregate_call(220); 12];
    let results = rpc.call_many(&calls).await.unwrap();
    assert_eq!(results.len(), 12);
    assert_eq!(server.requests(), 2);
    assert!(
        server
            .sizes
            .lock()
            .unwrap()
            .iter()
            .all(|&size| size <= MAX_DOCUMENT_BYTES)
    );
    // A call that cannot fit one body is refused before anything is sent.
    let oversized = [aggregate_call(2_400)];
    assert_eq!(
        rpc.call_many(&oversized).await,
        Err(ChainError::Transport(TransportError::Invalid))
    );
    assert_eq!(server.requests(), 2);
    // Batches stay on.
    rpc.call_many(&calls[..2]).await.unwrap();
    assert_eq!(server.requests(), 3);
}

#[tokio::test]
async fn a_block_the_rpc_has_not_imported_is_lag_not_a_failure() {
    // Without Multicall3 a refresh is the head, then reads pinned to its hash.
    let server = Server::start(Chain::new(false));
    let follower = follower(&server).await;
    let accepted = follower.current().unwrap();
    server.chain.head.fetch_add(1, Ordering::SeqCst);
    // Arc's -32014 for each pinned read, repeated past the reader's repeats:
    // the backend "hasn't yet imported that block" (docs.arc.io/arc/references/rpc-endpoints).
    server.chain.unimported.store(true, Ordering::SeqCst);
    let kept = follower.refresh().await.unwrap();
    assert!(Arc::ptr_eq(&accepted, &kept));
    assert!(follower.readiness(Some(session_id())).ready());
    // A watch the accepted snapshot cannot cover reports the lag alone.
    assert_eq!(
        follower
            .watch_session(B256::repeat_byte(0x56))
            .await
            .unwrap_err(),
        ChainError::Lagging
    );
    assert!(follower.readiness(Some(session_id())).ready());
    server.chain.unimported.store(false, Ordering::SeqCst);
    let next = follower.refresh().await.unwrap();
    assert_eq!(
        next.block().observation().block_number(),
        server.chain.head.load(Ordering::SeqCst)
    );
    follower.stop().await.unwrap();
}
