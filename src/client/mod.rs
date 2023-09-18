//! Contains the client side core protocol logic of `febft`.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use std::time::Instant;

use futures_timer::Delay;
use intmap::IntMap;
use log::{debug, error, info};

use atlas_common::{async_runtime, channel};
use atlas_common::channel::ChannelSyncRx;
use atlas_common::crypto::hash::Digest;
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::{Orderable, SeqNo};
use atlas_communication::{FullNetworkNode, NodeConnections};
use atlas_communication::protocol_node::{NodeIncomingRqHandler, ProtocolNetworkNode};
use atlas_core::messages::{Message, ReplyMessage, SystemMessage};
use atlas_core::ordering_protocol::OrderProtocolTolerance;
use atlas_core::ordering_protocol::reconfigurable_order_protocol::ReconfigurableOrderProtocol;
use atlas_core::reconfiguration_protocol::{QuorumUpdateMessage, ReconfigurableNodeTypes, ReconfigurationProtocol};
use atlas_core::serialize::{ClientMessage, ClientServiceMsg};
use atlas_core::timeouts::Timeouts;
use atlas_execution::serialize::ApplicationData;
use atlas_execution::system_params::SystemParams;
use atlas_metrics::benchmarks::ClientPerf;
use atlas_metrics::metrics::{metric_duration, metric_increment};

use crate::metric::{CLIENT_RQ_DELIVER_RESPONSE_ID, CLIENT_RQ_LATENCY_ID, CLIENT_RQ_PER_SECOND_ID, CLIENT_RQ_RECV_PER_SECOND_ID, CLIENT_RQ_RECV_TIME_ID, CLIENT_RQ_SEND_TIME_ID, CLIENT_RQ_TIMEOUT_ID};

use self::unordered_client::{FollowerData, UnorderedClientMode};

//pub mod observing_client;
pub mod ordered_client;
pub mod unordered_client;

macro_rules! certain {
    ($some:expr) => {
        match $some {
            Some(x) => x,
            None => unreachable!(),
        }
    };
}

struct SentRequestInfo {
    // The time at which this request was sent
    sent_time: Instant,
    //The amount of replicas/followers we sent the request to
    target_count: usize,
    //The amount of responses that we need to deliver the received response to the application
    //Delivers the response to the client when we have # of equal received responses >= responses_needed
    responses_needed: usize,
}

///Represents the possible ways a client can be notified of replies that were delivered to him
enum ClientAwaker<P> {
    //Callbacks have to be set from the start, since they are passed along with the request when the client issues it
    //So we always contain the callback struct
    Callback(Callback<P>),
    //Requests performed asynchronously however are a bit different. There can be 2 possibilities:
    // Client makes request, performs await (which populates the ready option) and then the responses are received and delivered to the client
    // Client makes requests and does not immediately perform await, the responses are received and the ready is populated with the responses
    // When the client awaits, it will see that there is already a Ready populated so it instantly delivers the payload to the client.
    Async(Option<Ready<P>>),
}

struct Ready<P> {
    waker: Option<Waker>,
    payload: Option<Result<P>>,
    timed_out: AtomicBool,
}

struct Callback<P> {
    to_call: Box<dyn FnOnce(Result<P>) + Send>,
    timed_out: AtomicBool,
}

impl<P> Deref for Callback<P> {
    type Target = Box<dyn FnOnce(Result<P>) + Send>;

    fn deref(&self) -> &Self::Target {
        &self.to_call
    }
}

pub struct ClientData<RF, D>
    where
        D: ApplicationData + 'static,
{
    //The global session counter, so we don't have two client objects with the same session number
    session_counter: AtomicU32,

    //Follower data
    follower_data: FollowerData,

    //Information about the requests that were sent like to how many replicas
    //they sent to, how many responses they need, etc
    request_info: Vec<Mutex<IntMap<SentRequestInfo>>>,

    //The ready items for requests made by this client. This is what is going to be used by the message receive task
    //To call the awaiting tasks
    ready: Vec<Mutex<IntMap<ClientAwaker<D::Reply>>>>,

    //Reconfiguration protocol
    reconfig_protocol: RF,

    // Receive messages from the reconfiguration protocol
    reconfig_protocol_rx: ChannelSyncRx<QuorumUpdateMessage>,

    /*//We only want to have a single observer client for any and all sessions that the user
    //May have, so we keep this reference in here
    observer: Arc<Mutex<Option<ObserverClient>>>,
    observer_ready: Mutex<Option<observing_client::Ready>>,*/

    stats: Option<Arc<ClientPerf>>,
}

pub trait ClientType<RF, D, NT> where D: ApplicationData + 'static {
    ///Initialize request in accordance with the type of clients
    fn init_request(
        session_id: SeqNo,
        operation_id: SeqNo,
        operation: D::Request,
    ) -> ClientMessage<D>;

    ///The return types for the iterator
    type Iter: Iterator<Item=NodeId>;

    ///Initialize the targets for the requests according with the type of request made
    ///
    /// Returns the iterator along with the amount of items contained within it
    fn init_targets(client: &Client<RF, D, NT>) -> (Self::Iter, usize);

    ///How many responses does that client need to get the
    fn needed_responses(client: &Client<RF, D, NT>) -> usize;
}

/// Represents a client node in `febft`.
// TODO: maybe make the clone impl more efficient
pub struct Client<RF, D, NT> where D: ApplicationData + 'static, NT: 'static {
    session_id: SeqNo,
    operation_counter: SeqNo,
    data: Arc<ClientData<RF, D>>,
    params: SystemParams,
    node: Arc<NT>,
}

impl<RF, D: ApplicationData, NT> Clone for Client<RF, D, NT> {
    fn clone(&self) -> Self {
        let session_id = self
            .data
            .session_counter
            .fetch_add(1, Ordering::Relaxed)
            .into();

        Self {
            session_id,
            params: self.params,
            node: self.node.clone(),
            data: Arc::clone(&self.data),
            //Start at one, since when receiving we check if received_op_id >= last_op_id, which is by default 0
            operation_counter: SeqNo::ZERO.next(),
        }
    }
}

pub(super) struct ClientRequestFut<'a, P> {
    request_key: u64,
    ready: &'a Mutex<IntMap<ClientAwaker<P>>>,
}

impl<'a, P> Future for ClientRequestFut<'a, P> {
    type Output = Result<P>;

    // TODO: maybe make this impl more efficient;
    // if we have a lot of requests being done in parallel,
    // the mutexes are going to have a fair bit of contention
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.ready
            .try_lock()
            .map(|mut ready| {
                let request = ready
                    .get_mut(self.request_key)
                    .expect("Request must be present here, how is this possible?");

                let request = match request {
                    ClientAwaker::Async(awaker) => {
                        if let None = awaker {
                            //If there is still no Ready inserted, insert now so it can later be fetched
                            *awaker = Some(Ready {
                                waker: None,
                                payload: None,
                                timed_out: AtomicBool::new(false),
                            });
                        }

                        //If there was already an inserted Ready struct, then the response is probably already ready to be received

                        awaker.as_mut().unwrap()
                    }
                    ClientAwaker::Callback(_) => unreachable!(),
                };

                if let Some(payload) = request.payload.take() {
                    //Response is ready, take it
                    ready.remove(self.request_key);
                    return Poll::Ready(payload);
                }

                // clone waker to wake up this task when
                // the response is ready
                request.waker = Some(cx.waker().clone());

                Poll::Pending
            })
            .unwrap_or_else(|_| {
                //Failed to get the lock, try again
                cx.waker().wake_by_ref();
                Poll::Pending
            })
    }
}

/// Represents a configuration used to bootstrap a `Client`.
pub struct ClientConfig<RF, D, NT> where
    RF: ReconfigurationProtocol + 'static,
    D: ApplicationData + 'static,
    NT: FullNetworkNode<RF::InformationProvider, RF::Serialization, ClientServiceMsg<D>> {
    pub n: usize,
    pub f: usize,

    pub unordered_rq_mode: UnorderedClientMode,

    /// Check out the docs on `NodeConfig`.
    pub node: NT::Config,

    pub reconfiguration: RF::Config,
}

///Keeps track of the replica (or follower, depending on the request mode) votes for a given request
pub struct ReplicaVotes {
    sent_time: Instant,
    //How many nodes did we contact in total with this request, so we can keep track if they have all responded
    contacted_nodes: usize,
    //How many votes do we need to provide a response?
    needed_votes_count: usize,
    //Which replicas have already voted?
    voted: BTreeSet<NodeId>,
    //The different digests we have received and how many times we have seen them
    digests: BTreeMap<Digest, usize>,
}

pub type RequestCallback<D: ApplicationData> = Box<dyn FnOnce(Result<D::Reply>) + Send>;

impl<D, RP, NT> Client<RP, D, NT>
    where
        RP: ReconfigurationProtocol + 'static,
        D: ApplicationData + 'static,
        NT: 'static
{
    /// Bootstrap a client in `febft`.
    pub async fn bootstrap<ROP>(cfg: ClientConfig<RP, D, NT>) -> Result<Self>
        where NT: FullNetworkNode<RP::InformationProvider, RP::Serialization, ClientServiceMsg<D>>,
              ROP: OrderProtocolTolerance + 'static, {
        let ClientConfig {
            n, f, node: node_config,
            unordered_rq_mode, reconfiguration,
        } = cfg;

        // system params
        let params = SystemParams::new(n, f)?;

        // Actually, we have to get the configuration from here

        let network_info_provider = RP::init_default_information(reconfiguration)?;

        // connect to peer nodes
        //
        // FIXME: can the client receive rogue reply messages?
        // perhaps when it reconnects to a replica after experiencing
        // network problems? for now ignore rogue messages...
        let node = Arc::new(NT::bootstrap(network_info_provider.clone(), node_config).await?);

        let default_timeout = Duration::from_secs(3);

        let (exec_tx, exec_rx) = channel::new_bounded_sync(128);

        let timeouts = Timeouts::new::<D>(node.id(), Duration::from_millis(1),
                                          default_timeout, exec_tx.clone());

        let (reconf_tx, reconf_rx) = channel::new_bounded_sync(128);

        // TODO: Make timeouts actually work properly with the clients (including making the normal
        //timeouts utilize this same system)

        let reconfig_protocol = RP::initialize_protocol(network_info_provider,
                                                        node.clone(),
                                                        timeouts.clone(),
                                                        ReconfigurableNodeTypes::ClientNode(reconf_tx),
                                                        ROP::get_n_for_f(1)).await?;

        info!("{:?} // Waiting for reconfiguration to stabilize...", node.id());

        match reconf_rx.recv() {
            Ok(message) => {
                match message {
                    QuorumUpdateMessage::UpdatedQuorumView(quorum_view) => {
                        info!("{:?} // Reconfiguration stabilized, quorum view: {:?}", node.id(), quorum_view);
                    }
                }
            }
            Err(err) => {
                return Err(Error::simple_with_msg(ErrorKind::ReconfigurationNotStable, format!("Error receiving reconfiguration message: {:?}", err).as_str()));
            }
        }

        let stats = {
            None
        };

        // create shared data
        let data = Arc::new(ClientData {
            session_counter: AtomicU32::new(0),
            follower_data: FollowerData::empty(unordered_rq_mode),

            request_info: std::iter::repeat_with(|| Mutex::new(IntMap::new()))
                .take(num_cpus::get())
                .collect(),

            ready: std::iter::repeat_with(|| Mutex::new(IntMap::new()))
                .take(num_cpus::get())
                .collect(),
            reconfig_protocol,
            reconfig_protocol_rx: reconf_rx,
            stats,
        });

        let task_data = Arc::clone(&data);

        let cli_node = node.clone();

        // spawn receiving task
        std::thread::Builder::new()
            .name(format!("Client {:?} message processing thread", node.id()))
            .spawn(move || Self::message_recv_task(params, task_data, node, timeouts, exec_rx))
            .expect("Failed to launch message processing thread");

        let session_id = data.session_counter.fetch_add(1, Ordering::Relaxed).into();

        info!("{:?} // Client has connected to all nodes and is ready to start", cli_node.id());

        Ok(Client {
            data,
            params,
            session_id,
            node: cli_node,
            //Start at one, since when receiving we check if received_op_id >= last_op_id, which is by default 0
            operation_counter: SeqNo::ZERO.next(),
        })
    }

    ///Bootstrap an observer client and get a reference to the observer client
    /*pub async fn bootstrap_observer(&mut self) -> &Arc<Mutex<Option<ObserverClient>>> {
        {
            let guard = self.data.observer.lock().unwrap();

            if let None = &*guard {
                drop(guard);

                let observer = ObserverClient::bootstrap_client::<D, NT>(self).await;

                let mut guard = self.data.observer.lock().unwrap();

                let _ = guard.insert(observer);
            }
        }

        &self.data.observer
    }*/
    #[inline]
    pub fn id(&self) -> NodeId where NT: ProtocolNetworkNode<ClientServiceMsg<D>> {
        self.node.id()
    }

    #[inline]
    pub fn session_id(&self) -> SeqNo {
        self.session_id
    }

    pub(super) fn client_data(&self) -> &Arc<ClientData<RP, D>> {
        &self.data
    }

    pub(super) fn get_quorum_view(&self) -> Vec<NodeId> {
        self.data.reconfig_protocol.get_quorum_members()
    }

    pub(super) fn update_inner<T>(&mut self, operation: D::Request) -> Result<ClientRequestFut<D::Reply>>
        where
            T: ClientType<RP, D, NT>,
            NT: ProtocolNetworkNode<ClientServiceMsg<D>>,
            RP: ReconfigurationProtocol + 'static {
        let start = Instant::now();

        let session_id = self.session_id;
        let operation_id = self.next_operation_id();

        let request_key = get_request_key(session_id, operation_id);

        let message = T::init_request(session_id, operation_id, operation);

        let request_info = get_request_info(session_id, &*self.data);

        //get the targets that we are supposed to broadcast the message to
        let (targets, target_count) = T::init_targets(&self);

        let sent_info = SentRequestInfo {
            sent_time: Instant::now(),
            target_count,
            responses_needed: T::needed_responses(self),
        };

        {
            let mut request_info_guard = request_info.lock().unwrap();

            request_info_guard.insert(request_key, sent_info);
        }

        // broadcast our request to the node group
        self.node.broadcast(message, targets);

        // await response
        let ready = get_ready::<RP, D>(session_id, &*self.data);

        {
            let mut ready_stored = ready.lock().unwrap();

            ready_stored.insert(request_key, ClientAwaker::Async(None));
        }

        Self::start_timeout(
            self.node.clone(),
            session_id,
            operation_id,
            self.data.clone(),
        );

        metric_duration(CLIENT_RQ_SEND_TIME_ID, start.elapsed());
        metric_increment(CLIENT_RQ_PER_SECOND_ID, Some(1));

        Ok(ClientRequestFut { request_key, ready })
    }

    /// Updates the replicated state of the application running
    /// on top of `atlas`.
    pub async fn update<T>(&mut self, operation: D::Request) -> Result<D::Reply>
        where
            T: ClientType<RP, D, NT>,
            NT: ProtocolNetworkNode<ClientServiceMsg<D>>,
            RP: ReconfigurationProtocol + 'static
    {
        self.update_inner::<T>(operation)?.await
    }

    pub(super) fn update_callback_inner<T>(&mut self, operation: D::Request) -> u64 where
        T: ClientType<RP, D, NT>,
        NT: ProtocolNetworkNode<ClientServiceMsg<D>>,
        RP: ReconfigurationProtocol + 'static {
        let start = Instant::now();

        let session_id = self.session_id;

        let operation_id = self.next_operation_id();

        let message = T::init_request(session_id, operation_id, operation);

        // await response
        let request_key = get_request_key(session_id, operation_id);

        //We only send the message after storing the callback to prevent us receiving the result without having
        //The callback registered, therefore losing the response
        let (targets, target_count) = T::init_targets(&self);

        let request_info = get_request_info(session_id, &*self.data);

        let sent_info = SentRequestInfo {
            sent_time: Instant::now(),
            target_count,
            responses_needed: T::needed_responses(self),
        };

        {
            let mut request_info_guard = request_info.lock().unwrap();

            request_info_guard.insert(request_key, sent_info);
        }

        self.node.broadcast(message, targets);

        Self::start_timeout(
            self.node.clone(),
            session_id,
            operation_id,
            self.data.clone(),
        );

        metric_duration(CLIENT_RQ_SEND_TIME_ID, start.elapsed());
        metric_increment(CLIENT_RQ_PER_SECOND_ID, Some(1));

        request_key
    }


    ///Update the SMR state with the given operation
    /// The callback should be a function to execute when we receive the response to the request.
    ///
    /// FIXME: This callback is going to be executed in an important thread for client performance,
    /// So in the callback, we should not perform any heavy computations / blocking operations as that
    /// will hurt the performance of the client. If you wish to perform heavy operations, move them
    /// to other threads to prevent slowdowns
    pub fn update_callback<T>(
        &mut self,
        operation: D::Request,
        callback: RequestCallback<D>,
    ) where
        T: ClientType<RP, D, NT>,
        NT: ProtocolNetworkNode<ClientServiceMsg<D>>,
        RP: ReconfigurationProtocol + 'static
    {
        let rq_key = self.update_callback_inner::<T>(operation);

        register_callback(self.session_id, rq_key, &*self.data, callback);
    }

    fn next_operation_id(&mut self) -> SeqNo {
        let id = self.operation_counter;

        self.operation_counter = self.operation_counter.next();

        id
    }

    ///Start performing a timeout for a given request.
    /// TODO: Repeat the request/do something else to fix this
    fn start_timeout(
        node: Arc<NT>,
        session_id: SeqNo,
        rq_id: SeqNo,
        client_data: Arc<ClientData<RP, D>>,
    ) where NT: ProtocolNetworkNode<ClientServiceMsg<D>> {
        let node = node.clone();

        async_runtime::spawn(async move {
            //Timeout delay
            Delay::new(Duration::from_secs(3)).await;

            let req_key = get_request_key(session_id, rq_id);

            {
                let bucket = get_ready::<RP, D>(session_id, &*client_data);

                let bucket_guard = bucket.lock().unwrap();

                let request = bucket_guard.get(req_key);

                if let Some(request) = request {
                    match request {
                        ClientAwaker::Callback(request) => {
                            request.timed_out.store(true, Ordering::SeqCst)
                        }
                        ClientAwaker::Async(Some(ready)) => {
                            ready.timed_out.store(true, Ordering::SeqCst)
                        }
                        ClientAwaker::Async(None) => {
                            //TODO: This has to be handled (should populate the ready with an empty, but timed out ready)
                            debug!("Weird timeout");
                        }
                    }

                    debug!("Request {:?} of session {:?} timed out", rq_id, session_id);

                    /*if let Some(sent_rqs) = &node.sent_rqs {
                        let bucket = &sent_rqs[req_key as usize % sent_rqs.len()];

                        if bucket.contains_key(&req_key) {
                            error!("{:?} // Request {:?} of session {:?} was SENT and timed OUT! Request key {}", node_id,
                    rq_id, session_id,get_request_key(session_id, rq_id));
                        };
                    } else {
                        error!("{:?} // Request {:?} of session {:?} was NOT SENT and timed OUT! Request key {}", node_id,
                    rq_id, session_id, get_request_key(session_id, rq_id));
                    }*/
                } else {
                    /*if let Some(sent_rqs) = &node.sent_rqs {
                        //Cleanup
                        let bucket = &sent_rqs[req_key as usize % sent_rqs.len()];

                        bucket.remove(&req_key);
                    }*/
                }

                metric_increment(CLIENT_RQ_TIMEOUT_ID, Some(1));
            }
        });
    }

    /// Create the default replica vote struct
    fn create_replica_votes(
        request_info: &Mutex<IntMap<SentRequestInfo>>,
        request_key: u64,
        params: &SystemParams,
    ) -> ReplicaVotes {
        let mut request_info_guard = request_info.lock().unwrap();

        let rq_info = request_info_guard.remove(request_key);

        if let Some(rq_info) = rq_info {
            //If we have information about the request in question,
            //Utilize it
            ReplicaVotes {
                sent_time: rq_info.sent_time,
                contacted_nodes: rq_info.target_count,
                needed_votes_count: rq_info.responses_needed,
                voted: Default::default(),
                digests: Default::default(),
            }
        } else {
            //If there is no stored information, take the safe road and require f + 1 votes
            ReplicaVotes {
                sent_time: Instant::now(),
                contacted_nodes: params.n(),
                needed_votes_count: params.f() + 1,
                voted: Default::default(),
                digests: Default::default(),
            }
        }
    }

    ///Deliver the reponse to the client
    fn deliver_response(
        node_id: NodeId,
        request_key: u64,
        vote: ReplicaVotes,
        ready: &Mutex<IntMap<ClientAwaker<D::Reply>>>,
        message: ReplyMessage<D::Reply>,
    ) {
        let start = Instant::now();

        let mut ready_lock = ready.lock().unwrap();

        let request = ready_lock.get_mut(request_key);

        let (session_id, operation_id, payload) = message.into_inner();

        if let Some(request) = request {
            match request {
                ClientAwaker::Callback(_) => {
                    //This is impossible to fail since we are in the Some method of the request
                    let request = ready_lock.remove(request_key).unwrap();

                    let request = match request {
                        ClientAwaker::Callback(request) => request,
                        _ => unreachable!(),
                    };

                    if request.timed_out.load(Ordering::Relaxed) {
                        error!(
                            "{:?} // Received response to timed out request {:?} on session {:?}",
                            node_id, session_id, operation_id
                        );
                    }

                    (request.to_call)(Ok(payload));
                }
                ClientAwaker::Async(opt_ready) => {
                    if let Some(request) = opt_ready.as_mut() {
                        // register response
                        request.payload = Some(Ok(payload));

                        if request.timed_out.load(Ordering::Relaxed) {
                            error!("{:?} // Received response to timed out request {:?} on session {:?}",
                                        node_id, session_id, operation_id);
                        }

                        // try to wake up the waiting task
                        if let Some(waker) = request.waker.take() {
                            waker.wake();
                        }
                    } else {
                        let request = Ready {
                            waker: None,
                            payload: Some(Ok(payload)),
                            timed_out: AtomicBool::new(false),
                        };

                        //populate the data with the received payload, even though the
                        //

                        *opt_ready = Some(request);
                    }
                }
            }
        } else {
            error!("Failed to get awaker for request {:?}", request_key)
        }

        metric_duration(CLIENT_RQ_DELIVER_RESPONSE_ID, start.elapsed());
        metric_duration(CLIENT_RQ_LATENCY_ID, vote.sent_time.elapsed());
        metric_increment(CLIENT_RQ_RECV_PER_SECOND_ID, Some(1));
    }

    ///Deliver an error response
    fn deliver_error(
        node_id: NodeId,
        request_key: u64,
        ready: &Mutex<IntMap<ClientAwaker<D::Reply>>>,
        (session_id, operation_id): (SeqNo, SeqNo),
    ) {
        let mut ready_lock = ready.lock().unwrap();

        let request = ready_lock.get_mut(request_key);

        let err_msg = Err("Could not get f+1 equal responses, failed to execute the request")
            .wrapped(ErrorKind::CoreClient);

        if let Some(request) = request {
            match request {
                ClientAwaker::Callback(_) => {
                    //This is impossible to fail since we are in the Some method of the request
                    let request = ready_lock.remove(request_key).unwrap();

                    let request = match request {
                        ClientAwaker::Callback(request) => request,
                        _ => unreachable!(),
                    };

                    if request.timed_out.load(Ordering::Relaxed) {
                        error!(
                            "{:?} // Received response to timed out request {:?} on session {:?}",
                            node_id, session_id, operation_id
                        );
                    }

                    (request.to_call)(err_msg);
                }
                ClientAwaker::Async(opt_ready) => {
                    if let Some(request) = opt_ready.as_mut() {
                        // register response
                        request.payload = Some(err_msg);

                        if request.timed_out.load(Ordering::Relaxed) {
                            error!("{:?} // Received response to timed out request {:?} on session {:?}",
                                    node_id, session_id, operation_id);
                        }

                        // try to wake up the waiting task
                        if let Some(waker) = request.waker.take() {
                            waker.wake();
                        }
                    } else {
                        let request = Ready {
                            waker: None,
                            payload: Some(err_msg),
                            timed_out: AtomicBool::new(false),
                        };

                        //populate the data with the received payload
                        *opt_ready = Some(request);
                    }
                }
            }
        } else {
            error!("Failed to get awaker for request {:?}", request_key)
        }
    }

    ///This task might become a large bottleneck with the scenario of few clients with high concurrent rqs,
    /// As the replicas will make very large batches and respond to all the sent requests in one go.
    /// This leaves this thread with a very large task to do in a very short time and it just can't keep up
    fn message_recv_task(params: SystemParams, data: Arc<ClientData<RP, D>>,
                         node: Arc<NT>, timeouts: Timeouts, timeout_rx: ChannelSyncRx<Message>) where NT: ProtocolNetworkNode<ClientServiceMsg<D>> {
        // use session id as key
        let mut last_operation_ids: IntMap<SeqNo> = IntMap::new();
        let mut replica_votes: IntMap<ReplicaVotes> = IntMap::new();

        //TODO: Maybe change this to make clients use the same timeouts service?
        while let Ok(message) = node.node_incoming_rq_handling().receive_from_replicas(None) {
            let start = Instant::now();

            let (header, sys_msg) = message.unwrap().into_inner();

            match &sys_msg {
                SystemMessage::OrderedReply(msg_info)
                | SystemMessage::UnorderedReply(msg_info) => {
                    let session_id = msg_info.session_id();
                    let operation_id = msg_info.sequence_number();

                    //Check if we have already executed the operation
                    let last_operation_id = last_operation_ids
                        .get(session_id.into())
                        .copied()
                        .unwrap_or(SeqNo::ZERO);

                    // reply already delivered to application
                    if last_operation_id >= operation_id {
                        continue;
                    }

                    let request_key = get_request_key(session_id, operation_id);

                    //Get the votes for the instance
                    let votes = IntMapEntry::get(request_key, &mut replica_votes)
                        .or_insert_with(|| {
                            let request_info = get_request_info(session_id, &*data);

                            Self::create_replica_votes(request_info, request_key, &params)
                        });

                    //Check if replicas try to vote twice on the same consensus instance
                    if votes.voted.contains(&header.from()) {
                        error!("{:?} // Replica {:?} voted twice for the same request, ignoring!",
                            header.to(),header.from());

                        continue;
                    }

                    votes.voted.insert(header.from());

                    //Get how many equal responses we have received and see if we can deliver the state to the client
                    let count = if votes.digests.contains_key(header.digest()) {
                        //Increment the amount of votes that reply has
                        *(votes.digests.get_mut(header.digest()).unwrap()) += 1;

                        votes.digests.get(header.digest()).unwrap().clone()
                    } else {
                        //Register the newly received reply (has not been seen yet)
                        votes.digests.insert(header.digest().clone(), 1);

                        1
                    };

                    // wait for the amount of votes that we require identical replies
                    // In a BFT system, this is by default f+1
                    if count >= votes.needed_votes_count {
                        let votes = replica_votes.remove(request_key).unwrap();

                        last_operation_ids.insert(session_id.into(), operation_id);

                        //Get the wakers for this request and deliver the payload

                        let ready = get_ready::<RP, D>(session_id, &*data);

                        Self::deliver_response(
                            node.id(),
                            request_key,
                            votes,
                            ready,
                            match sys_msg {
                                SystemMessage::OrderedReply(message)
                                | SystemMessage::UnorderedReply(message) => message,
                                _ => unreachable!(),
                            },
                        );
                    } else {
                        //If we do not have f+1 replies yet, check if it's still possible to get those
                        //Replies by taking a look at the target count and currently received replies count

                        let mut total_count: usize = 0;

                        for (_, count) in votes.digests.iter() {
                            total_count += count;
                        }

                        if total_count >= votes.contacted_nodes {
                            //We already got all of our responses, so it's impossible to get f+1 equal reponses
                            //What we will do now is call the awakers with an Err result
                            replica_votes.remove(request_key);

                            //Get the wakers for this request and deliver the payload
                            let ready = get_ready::<RP, D>(session_id, &*data);

                            Self::deliver_error(
                                node.id(),
                                request_key,
                                ready,
                                (session_id, operation_id),
                            );
                        }
                    }

                    metric_duration(CLIENT_RQ_RECV_TIME_ID, start.elapsed());

                    continue;
                }
                _ => {}
            }

            Self::receive_from_timeouts(&data, &timeout_rx)
        }
    }

    fn receive_from_timeouts(data: &Arc<ClientData<RP, D>>, exec_rx: &ChannelSyncRx<Message>) {
        while let Ok(timeout) = exec_rx.try_recv() {
            match timeout {
                Message::Timeout(timeout) => {
                    data.reconfig_protocol.handle_timeout(timeout).expect("Failed to deliver timeout");
                }
                _ => {
                    todo!()
                }
            }
        }
    }

    fn receive_reconf_updates(data: &Arc<ClientData<RP, D>>) {
        while let Ok(update) = data.reconfig_protocol_rx.try_recv() {
            match update {
                QuorumUpdateMessage::UpdatedQuorumView(update) => {}
                _ => {
                    todo!()
                }
            }
        }
    }
}

#[inline]
fn get_request_key(session_id: SeqNo, operation_id: SeqNo) -> u64 {
    let sess: u64 = session_id.into();
    let opid: u64 = operation_id.into();
    sess | (opid << 32)
}

#[inline]
fn get_correct_vec_for<T>(session_id: SeqNo, vec: &Vec<Mutex<T>>) -> &Mutex<T> {
    let session_id: usize = session_id.into();
    let index = session_id % vec.len();

    &vec[index]
}

#[inline]
pub(super) fn register_callback<RF, D: ApplicationData>(session_id: SeqNo,
                                                        request_key: u64,
                                                        data: &ClientData<RF, D>,
                                                        callback: RequestCallback<D>) {
    let ready = get_ready::<RF, D>(session_id, &*data);

    let callback = Callback {
        to_call: callback,
        timed_out: AtomicBool::new(false),
    };

    //Scope the mutex operations to reduce the lifetime of the guard
    {
        let mut ready_callback_guard = ready.lock().unwrap();

        ready_callback_guard.insert(request_key, ClientAwaker::Callback(callback));
    }
}

#[inline]
fn get_ready<RF, D: ApplicationData>(
    session_id: SeqNo,
    data: &ClientData<RF, D>,
) -> &Mutex<IntMap<ClientAwaker<D::Reply>>> {
    get_correct_vec_for(session_id, &data.ready)
}

#[inline]
fn get_request_info<RF, D: ApplicationData>(
    session_id: SeqNo,
    data: &ClientData<RF, D>,
) -> &Mutex<IntMap<SentRequestInfo>> {
    get_correct_vec_for(session_id, &data.request_info)
}

struct IntMapEntry<'a, T> {
    key: u64,
    map: &'a mut IntMap<T>,
}

impl<'a, T> IntMapEntry<'a, T> {
    fn get(key: u64, map: &'a mut IntMap<T>) -> Self {
        Self { key, map }
    }

    fn or_insert_with<F: FnOnce() -> T>(self, default: F) -> &'a mut T {
        let (key, map) = (self.key, self.map);

        if !map.contains_key(key) {
            let value = (default)();
            map.insert(key, value);
        }

        certain!(map.get_mut(key))
    }
}
