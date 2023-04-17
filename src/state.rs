use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use rand::Rng;
use tokio::sync::Mutex;

pub type Clients = Arc<Mutex<HashMap<SocketAddr, Client>>>;

#[derive(Debug, Clone)]
pub enum Client {
    Direct(String, Option<SocketAddr>, Option<bool>),
    MatchMaking(String, Vec<Option<SocketAddr>>),
}

impl Client {
    pub fn new_direct(code: String) -> Self {
        Self::Direct(code, None, None)
    }

    pub fn get_code(&self) -> &str {
        match self {
            Self::Direct(code, _, _) => code,
            Self::MatchMaking(code, _) => code,
        }
    }

    pub fn get_addr(&self) -> Option<SocketAddr> {
        match self {
            Self::Direct(_, addr, _) => *addr,
            Self::MatchMaking(_, addrs) => addrs[0],
        }
    }

    pub fn set_addr(&mut self, to_update: SocketAddr) {
        match self {
            Self::Direct(_, addr, _) => *addr = Some(to_update),
            Self::MatchMaking(_, addrs) => addrs[0] = Some(to_update),
        }
    }

    pub fn is_server(&self) -> Option<bool> {
        match self {
            Self::Direct(_, _, is_server) => *is_server,
            Self::MatchMaking(_, _) => None,
        }
    }

    pub fn set_is_server(&mut self, to_update: bool) {
        match self {
            Self::Direct(_, _, is_server) => *is_server = Some(to_update),
            Self::MatchMaking(_, _) => {}
        }
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
    async fn register_direct_async(&mut self, addr: SocketAddr) -> bool;
    async fn get_client(&self, addr: SocketAddr) -> Option<Client>;
    async fn remove_client(&mut self, addr: SocketAddr) -> Option<Client>;
}

#[async_trait]
impl ClientsExts for Clients {
    async fn register_direct_async(&mut self, addr: SocketAddr) -> bool {
        let mut clients = self.lock().await;

        if clients.contains_key(&addr) {
            eprintln!("addr {}  already registered", addr);
            return false;
        }

        if clients
            .iter()
            .any(|(k, _)| k.ip() == addr.ip() && addr.ip().to_string() != "127.0.0.1")
        {
            eprintln!("ip {} already registered", addr.ip());
            return false;
        }

        //generate random code for direct connection
        let mut code = rand::thread_rng()
            .sample_iter(&rand::distributions::Alphanumeric)
            .take(5)
            .map(char::from)
            .collect::<String>();

        while clients.values().any(|client| client.get_code() == &code) {
            code = rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(5)
                .map(char::from)
                .collect::<String>();
        }

        clients.insert(addr, Client::new_direct(code));
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
