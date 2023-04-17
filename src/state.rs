use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use rand::Rng;
use tokio::sync::Mutex;

pub type DirectClients = Arc<Mutex<HashMap<SocketAddr, String>>>;

#[derive(Debug, Clone)]
pub struct AppState {
    pub clients_registered_for_direct: DirectClients,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            clients_registered_for_direct: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
pub trait DirectClientsExts {
    async fn register_direct_async(&mut self, addr: SocketAddr) -> bool;
}

#[async_trait]
impl DirectClientsExts for DirectClients {
    async fn register_direct_async(&mut self, addr: SocketAddr) -> bool {
        let mut registerd = self.lock().await;

        if registerd.contains_key(&addr) {
            eprintln!("addr {}  already registered", addr);
            return false;
        }

        //generate random code for direct connection
        let mut code = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(5)
            .map(char::from)
            .collect::<String>();

        while registerd.values().any(|v| v == &code) {
            code = rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(5)
                .map(char::from)
                .collect::<String>();
        }

        if registerd
            .iter()
            .any(|(k, _)| k.ip() == addr.ip() && addr.ip().to_string() != "127.0.0.1")
        {
            eprintln!("ip {} already registered", addr.ip());
            return false;
        }

        registerd.insert(addr, code);
        true
    }
}
