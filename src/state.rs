use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use rand::Rng;
use tokio::sync::Mutex;

pub type Clients = Arc<Mutex<HashMap<SocketAddr, Client>>>;

#[derive(Debug, Clone)]
pub struct Client {
    pub code: String,
    pub public_ip: SocketAddr,
    pub typ: MatchmakingType,
    pub matched: Option<SocketAddr>,
    pub is_server: Option<bool>,
}

#[derive(Debug, Clone)]
pub enum MatchmakingType {
    Direct,
    MatchMaking,
}

impl Client {
    pub fn new_direct(code: String, public_ip: SocketAddr) -> Self {
        Self {
            code,
            public_ip,
            typ: MatchmakingType::Direct,
            matched: None,
            is_server: None,
        }
    }

    pub fn set_matched(&mut self, to_update: SocketAddr) {
        self.matched = Some(to_update);
    }

    pub fn is_server(&self) -> Option<bool> {
        self.is_server
    }

    pub fn set_is_server(&mut self, to_update: bool) {
        self.is_server = Some(to_update);
    }
}

#[derive(Debug, Clone)]
pub struct AppState {
    pub clients: Clients,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
pub trait ClientsExts {
    async fn register_direct_async(
        &mut self,
        private_ip: SocketAddr,
        public_ip: SocketAddr,
    ) -> bool;
    async fn get_client(&self, addr: SocketAddr) -> Option<Client>;
    async fn remove_client(&mut self, addr: SocketAddr) -> Option<Client>;
}

#[async_trait]
impl ClientsExts for Clients {
    async fn register_direct_async(
        &mut self,
        private_ip: SocketAddr,
        public_ip: SocketAddr,
    ) -> bool {
        let mut clients = self.lock().await;

        if clients.contains_key(&private_ip) {
            eprintln!("addr {}  already registered", private_ip);
            return false;
        }

        if clients
            .iter()
            .any(|(k, _)| k.ip() == private_ip.ip() && private_ip.ip().to_string() != "127.0.0.1")
        {
            eprintln!("ip {} already registered", private_ip.ip());
            return false;
        }

        //generate random code for direct connection
        let mut code = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(5)
            .map(char::from)
            .collect::<String>();

        while clients.values().any(|client| client.code == code) {
            code = rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(5)
                .map(char::from)
                .collect::<String>();
        }

        clients.insert(private_ip, Client::new_direct(code, public_ip));
        true
    }

    async fn get_client(&self, addr: SocketAddr) -> Option<Client> {
        let clients = self.lock().await;
        clients.get(&addr).cloned()
    }

    async fn remove_client(&mut self, addr: SocketAddr) -> Option<Client> {
        let mut clients = self.lock().await;
        clients.remove(&addr)
    }
}
