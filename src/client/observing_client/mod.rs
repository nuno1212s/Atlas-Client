use core::task::{Context, Waker};
use log::{error, warn};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use atlas_common::node_id::NodeId;
use febft_pbft_consensus::bft::message::{ObserveEventKind, ObserverMessage, PBFTMessage};
use atlas_common::ordering::Orderable;
use atlas_communication::message::{Header, NetworkMessage, NetworkMessageKind};
use atlas_communication::{Node};
use atlas_execution::serialize::SharedData;
use atlas_core::messages::{Message, Protocol, SystemMessage};
use febft_pbft_consensus::bft::message::serialize::PBFTConsensus;
use febft_pbft_consensus::bft::PBFT;
use crate::client::{Client, ClientData};

///Callback to when the replicas send their notifications
///When a new observe event is received, this function will be executed
pub trait ObserverCallback {
    fn handle_event(&self, event: ObserveEventKind, n: usize);
}

const QUORUM: usize = 3;

///Structure to hold all of the currently registered callbacks to know
///where to deliver the messages
pub struct ObserverClient {
    registered_callbacks: Vec<Box<dyn ObserverCallback + Send + 'static>>,
    registered_callback_fns: Vec<Box<fn(ObserveEventKind, usize)>>,

    //The messages that we have received and the replicas that sent them
    //This is because we only want to deliver messages when we get 2f+1, as that's the only
    //Time where we can guarantee that the observation is the correct one.
    received_observations: Vec<(ObserveEventKind, Vec<NodeId>)>,
}

impl ObserverClient {
    pub async fn bootstrap_client<D, NT>(client: &mut Client<D, NT>) -> ObserverClient
        where
            D: SharedData + 'static,
            NT: Node<PBFT<D>> + 'static
    {
        let targets = NodeId::targets(0..client.params.n());

        //Register the observer clients with the client node
        client.node.broadcast(
            NetworkMessageKind::from(SystemMessage::from_protocol_message(PBFTMessage::ObserverMessage(ObserverMessage::ObserverRegister))),
            targets,
        );

        PendingObserverRequestFut {
            responses_needed: 0,
            ready: &client.data.observer_ready,
        }
            .await;

        ObserverClient {
            registered_callbacks: Vec::new(),
            registered_callback_fns: Vec::new(),

            received_observations: vec![],
        }
    }

    pub fn register_observer(&mut self, callback: Box<dyn ObserverCallback + Send + 'static>) {
        self.registered_callbacks.push(callback);
    }

    pub fn register_observer_fn(&mut self, callback: Box<fn(ObserveEventKind, usize)>) {
        self.registered_callback_fns.push(callback)
    }

    pub(super) fn handle_observed_message<D>(
        client_data: &Arc<ClientData<D>>,
        header: Header,
        observed_msg: Protocol<PBFTMessage<D::State, D::Request>>,
    ) where
        D: SharedData + 'static,
    {

        match observed_msg.into_inner() {
            PBFTMessage::ObserverMessage(observed_msg) => {
                match observed_msg {
                    ObserverMessage::ObserverRegister
                    | ObserverMessage::ObserverUnregister => {
                        warn!("Cannot register at the client side???");
                    }
                    ObserverMessage::ObserverRegisterResponse(success) => {
                        if success {
                            let mut guard = client_data.observer_ready.lock().unwrap();

                            let ready = match &mut *guard {
                                None => guard.insert(Ready {
                                    waker: None,
                                    responses_received: Default::default(),
                                }),
                                Some(ready) => ready,
                            };

                            ready.responses_received.fetch_add(1, Ordering::SeqCst);

                            if let Some(waker) = &ready.waker {
                                //Since we don't have access to the necessary number of responses
                                //We just wake the thread to check if it's done
                                waker.wake_by_ref();
                            }
                        }
                    }
                    ObserverMessage::ObservedValue(value) => {
                        let mut result = client_data.observer.lock().unwrap();

                        //Since there probably won't be much contention in this lock
                        //as this will only be accessed when registering the observer
                        //And when delivering requests (and that's only done on the message processing thread of each client
                        //So only one thread will access it at once for most of the time
                        if let Some(observer) = &mut *result {
                            for i in 0..observer.received_observations.len() {
                                let should_remove = {
                                    let (event, sent) =
                                        observer.received_observations.get_mut(i).unwrap();

                                    match (&value, event) {
                                        (
                                            ObserveEventKind::CheckpointStart(seq),
                                            ObserveEventKind::CheckpointStart(seq2),
                                        ) if seq == seq2 => {}
                                        (
                                            ObserveEventKind::CheckpointEnd(seq),
                                            ObserveEventKind::CheckpointEnd(seq2),
                                        ) if seq == seq2 => {}
                                        (
                                            ObserveEventKind::Ready(seq),
                                            ObserveEventKind::Ready(seq2),
                                        ) if seq == seq2 => {}
                                        (
                                            ObserveEventKind::Prepare(seq),
                                            ObserveEventKind::Prepare(seq2),
                                        ) if seq == seq2 => {}
                                        (
                                            ObserveEventKind::Commit(seq),
                                            ObserveEventKind::Commit(seq2),
                                        ) if seq == seq2 => {}
                                        (
                                            ObserveEventKind::Executed(seq),
                                            ObserveEventKind::Executed(seq2),
                                        ) if seq == seq2 => {}
                                        (
                                            ObserveEventKind::NormalPhase((view, seq)),
                                            ObserveEventKind::NormalPhase((view2, seq2)),
                                        ) if seq == seq2
                                            && view.sequence_number()
                                            == view2.sequence_number() => {}
                                        (
                                            ObserveEventKind::ViewChangePhase,
                                            ObserveEventKind::ViewChangePhase,
                                        ) => {}
                                        (
                                            ObserveEventKind::CollabStateTransfer,
                                            ObserveEventKind::CollabStateTransfer,
                                        ) => {}
                                        (_, _) => {
                                            continue;
                                        }
                                    }

                                    if sent.contains(&header.from()) {
                                        error!("Repeat message received!");

                                        break;
                                    } else {
                                        sent.push(header.from());

                                        if sent.len() == QUORUM {
                                            //Deliver the observed
                                            for x in observer.registered_callbacks.iter() {
                                                x.handle_event(value.clone(), sent.len());
                                            }

                                            for x in observer.registered_callback_fns.iter()
                                            {
                                                x(value.clone(), sent.len());
                                            }

                                            false
                                        } else if sent.len() > QUORUM {
                                            //Deliver the observed
                                            for x in observer.registered_callbacks.iter() {
                                                x.handle_event(value.clone(), sent.len());
                                            }

                                            for x in observer.registered_callback_fns.iter()
                                            {
                                                x(value.clone(), sent.len());
                                            }

                                            true
                                        } else {
                                            false
                                        }
                                    }
                                };

                                if should_remove {
                                    observer.received_observations.remove(i);
                                }

                                break;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
}

pub struct Ready {
    waker: Option<Waker>,
    responses_received: AtomicU32,
}

struct PendingObserverRequestFut<'a> {
    responses_needed: u32,
    //Reference to the ready value for this future observer
    ready: &'a Mutex<Option<Ready>>,
}

impl<'a> Future for PendingObserverRequestFut<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.ready
            .try_lock()
            .map(|mut ready| {
                let rq = match &mut *ready {
                    None => ready.insert(Ready {
                        waker: None,
                        responses_received: AtomicU32::new(0),
                    }),
                    Some(ready) => ready,
                };

                //If we already have the required acks, we can allow the user to add callbacks
                if rq.responses_received.load(Ordering::SeqCst) > self.responses_needed {
                    ready.take();

                    return Poll::Ready(());
                }

                rq.waker = Some(cx.waker().clone());

                Poll::Pending
            })
            .unwrap_or_else(|_| {
                cx.waker().wake_by_ref();

                Poll::Pending
            })
    }
}
