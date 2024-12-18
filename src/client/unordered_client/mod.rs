use std::collections::BTreeSet;
use std::sync::Mutex;

use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::ordering::SeqNo;
use atlas_common::Err;
use atlas_communication::byte_stub::connections::NetworkConnectionController;
use atlas_communication::stub::NetworkStub;
use atlas_core::messages::RequestMessage;
use atlas_core::reconfiguration_protocol::ReconfigurationProtocol;
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_core::message::OrderableMessage;
use atlas_smr_core::networking::client::SMRClientNetworkNode;
use atlas_smr_core::serialize::SMRSysMessage;

use super::{Client, ClientError, ClientType};

pub enum UnorderedClientMode {
    ///BFT Client mode
    /// In this mode, clients ask at least f+1 followers the same request and will require us to
    /// receive f + 1 equal responses.
    /// If we are not able to receive f + 1 equal responses from all of the followers we asked we will deliver an error
    /// From there, the client can then decide whether to perform an ordered request or to try again
    BFT,

    ///Only needs to receive one response in order to deliver them to the client.
    /// This is not BFT as any follower can deliver a wrong response and the client will accept it
    BestEffort,
}

///Data relative to the follower mode of the client
pub(super) struct FollowerData {
    //The mode which the client is currently using
    unordered_request_mode: UnorderedClientMode,
    //The followers that we are currently connected to
    connected_followers: Mutex<BTreeSet<NodeId>>,
    //The followers we are currently attempting to connect to
    connecting_followers: Mutex<BTreeSet<NodeId>>,
}

impl FollowerData {
    pub fn empty(mode: UnorderedClientMode) -> Self {
        FollowerData {
            unordered_request_mode: mode,
            connected_followers: Mutex::new(Default::default()),
            connecting_followers: Mutex::new(Default::default()),
        }
    }
}

impl<RF, D, NT> Client<RF, D, NT>
where
    D: ApplicationData + 'static,
    RF: ReconfigurationProtocol + 'static,
{
    ///Connect to a follower with a given node id
    ///
    /// Returns Err if we are already connecting to or connected to
    /// the given follower.
    #[allow(dead_code)]
    fn connect_to_follower(&self, node_id: NodeId) -> Result<()>
    where
        NT: SMRClientNetworkNode<RF::InformationProvider, RF::Serialization, D>,
    {
        {
            let connecting = self.data.follower_data.connecting_followers.lock().unwrap();

            if connecting.contains(&node_id) {
                return Err!(ClientError::AlreadyConnectingToNode(node_id));
            }
        }

        {
            let connected = self.data.follower_data.connected_followers.lock().unwrap();

            if connected.contains(&node_id) {
                return Err!(ClientError::AlreadyConnectedToNode(node_id));
            }
        }

        let client_data = self.data.clone();

        let callback = Box::new(move |res| {
            let mut connecting_followers = client_data
                .follower_data
                .connecting_followers
                .lock()
                .unwrap();

            connecting_followers.remove(&node_id);

            if res {
                let mut connected_followers = client_data
                    .follower_data
                    .connected_followers
                    .lock()
                    .unwrap();

                connected_followers.insert(node_id);
            }
        });

        let _ = self.node.app_node().connections().connect_to_node(node_id);

        Ok(())
    }
}

pub struct Unordered;

impl<RF, D, NT> ClientType<RF, D, NT> for Unordered
where
    D: ApplicationData + 'static,
    RF: ReconfigurationProtocol + 'static,
{
    fn init_request(
        session_id: SeqNo,
        operation_id: SeqNo,
        operation: D::Request,
    ) -> SMRSysMessage<D> {
        OrderableMessage::UnorderedRequest(RequestMessage::new(session_id, operation_id, operation))
    }

    type Iter = impl Iterator<Item = NodeId>;

    fn init_targets(client: &Client<RF, D, NT>) -> (Self::Iter, usize) {
        /*
        //TODO: Atm we are using all followers, we should choose a small number of them and
        // Send it to those. (Maybe the ones that are closes? TBD)
        let connected_followers: Vec<NodeId> = client
            .data
            .follower_data
            .connected_followers
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect();

        let count = connected_followers.len();

        if count > 0 {
            return (connected_followers.into_iter(), count);
        } else {
            let connected: Vec<NodeId> = NodeId::targets(0..client.params.n()).collect();

            return (connected.into_iter(), client.params.n());
        };
        */
        let quorum = client.get_quorum_view();

        let quorum_len = quorum.len();

        (quorum.into_iter(), quorum_len)
    }

    fn needed_responses(client: &Client<RF, D, NT>) -> usize {
        let f = client.data.reconfig_protocol.get_current_f();

        match client.data.follower_data.unordered_request_mode {
            UnorderedClientMode::BFT => f + 1,
            UnorderedClientMode::BestEffort => 1,
        }
    }
}
