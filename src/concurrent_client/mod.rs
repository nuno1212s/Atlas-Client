use crate::client;
use atlas_common::channel::{ChannelSyncRx, ChannelSyncTx};
use atlas_common::error::*;
use atlas_common::node_id::NodeId;
use atlas_common::{channel, quiet_unwrap};
use atlas_communication::stub::RegularNetworkStub;
use atlas_core::ordering_protocol::OrderProtocolTolerance;
use atlas_core::reconfiguration_protocol::ReconfigurationProtocol;
use atlas_smr_application::serialize::ApplicationData;
use atlas_smr_core::networking::client::SMRClientNetworkNode;
use atlas_smr_core::serialize::SMRSysMsg;
use std::sync::{Arc, Mutex};
use tracing::error;

use crate::client::{get_request_key, ClientData};
use crate::client::{register_callback, Client, ClientConfig, ClientType, RequestCallback};

/// A client implementation that will automagically manage all the sessions required, re-utilizing them
/// as much as possible
/// Can be cloned in order to be used in multiple locations simultaneously
#[derive(Clone)]
pub struct ConcurrentClient<RF, D: ApplicationData + 'static, NT: 'static> {
    id: NodeId,
    client_data: Arc<ClientData<RF, D>>,
    session_return: ChannelSyncTx<Client<RF, D, NT>>,
    sessions: ChannelSyncRx<Client<RF, D, NT>>,
}

pub async fn bootstrap_client<RP, D, NT, ROP>(
    id: NodeId,
    cfg: ClientConfig<RP, D, NT>,
    session_limit: usize,
) -> Result<ConcurrentClient<RP, D, NT::AppNode>>
where
    RP: ReconfigurationProtocol + 'static,
    D: ApplicationData + 'static,
    NT: SMRClientNetworkNode<RP::InformationProvider, RP::Serialization, D> + 'static,
    ROP: OrderProtocolTolerance,
{
    // Creates a new concurrent client, with the given configuration
    let (tx, rx) = channel::new_bounded_sync(session_limit, None::<String>);

    let client = client::bootstrap_client::<RP, D, NT, ROP>(id, cfg).await?;

    let id = client.id();
    let data = client.client_data().clone();

    // Populate the channel with the given session limit
    for _ in 1..session_limit {
        tx.send_return(client.clone())?;
    }

    tx.send_return(client)?;

    Ok(ConcurrentClient {
        id,
        client_data: data,
        session_return: tx,
        sessions: rx,
    })
}

impl<RF, D, NT> ConcurrentClient<RF, D, NT>
where
    D: ApplicationData + 'static,
    RF: ReconfigurationProtocol,
    NT: 'static,
{
    /// Creates a new concurrent client, from an already existing client
    pub fn from_client(client: Client<RF, D, NT>, session_limit: usize) -> Result<Self>
    where
        NT: RegularNetworkStub<SMRSysMsg<D>>,
    {
        let (tx, rx) = channel::new_bounded_sync(session_limit, None::<String>);

        let id = client.id();
        let data = client.client_data().clone();

        // Populate the channel with the given session limit
        for _ in 1..session_limit {
            tx.send_return(client.clone())?;
        }

        tx.send_return(client)?;

        Ok(Self {
            id,
            client_data: data,
            session_return: tx,
            sessions: rx,
        })
    }

    #[inline]
    pub fn id(&self) -> NodeId {
        self.id
    }

    fn get_session(&self) -> Result<Client<RF, D, NT>> {
        self.sessions.recv()
    }

    /// Updates the replicated state of the application running
    /// on top of `atlas`.
    pub async fn update<T>(&self, request: D::Request) -> Result<D::Reply>
    where
        T: ClientType<RF, D, NT> + 'static,
        NT: RegularNetworkStub<SMRSysMsg<D>>,
    {
        let mut session = self.get_session()?;

        let result = session.update::<T>(request).await;

        self.session_return.send_return(session)?;

        result
    }

    ///Update the SMR state with the given operation
    /// The callback should be a function to execute when we receive the response to the request.
    ///
    /// FIXME: This callback is going to be executed in an important thread for client performance,
    /// So in the callback, we should not perform any heavy computations / blocking operations as that
    /// will hurt the performance of the client. If you wish to perform heavy operations, move them
    /// to other threads to prevent slowdowns
    pub fn update_callback<T>(
        &self,
        request: D::Request,
        callback: RequestCallback<D>,
    ) -> Result<()>
    where
        T: ClientType<RF, D, NT> + 'static,
        NT: RegularNetworkStub<SMRSysMsg<D>>,
    {
        let mut session = self.get_session()?;
        
        let session_id = session.session_id();

        let session_return = self.session_return.clone();

        let operation_id = session.next_operation_id();

        let rq_key = get_request_key(session_id, operation_id);

        let (return_session_tx, return_session_rx) = channel::new_oneshot_channel();

        let callback = Box::new(move |reply| {
            callback(reply);

            let client = return_session_rx.recv().expect("Failed to get returned session");
            
            quiet_unwrap!(session_return.send(client));
        });

        register_callback(session_id, rq_key, &*self.client_data, callback);

        session.update_callback_inner::<T>(request, operation_id);
        
        return_session_tx.send(session)?;
        
        Ok(())
    }
}
