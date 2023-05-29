pub mod client;
pub mod signaling_state;

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use tokio::sync::Mutex;

use crate::services::geoip::GeoipClient;

use self::{client::Client, signaling_state::SignalingState};

pub type Clients = Arc<Mutex<HashMap<SocketAddr, Client>>>;

#[derive(Debug, Clone)]
pub struct AppState {
    pub clients: Clients,
    geoip_client: GeoipClient,
    pub signaling: Arc<Mutex<SignalingState>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            geoip_client: GeoipClient::new(),
            signaling: Arc::new(Mutex::new(SignalingState::default())),
        }
    }

    pub fn get_geoip_client(&self) -> GeoipClient {
        self.geoip_client.clone()
    }
}
