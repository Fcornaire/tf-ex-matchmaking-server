use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use rand::Rng;
use tokio::sync::Mutex;
use tracing::error;

pub type Clients = Arc<Mutex<HashMap<SocketAddr, Client>>>;

#[derive(Debug, Clone)]
pub struct Client {
    pub code: String,
    pub public_ip: SocketAddr,
    pub matchmaking_type: MatchmakingType,
    pub matched: Option<SocketAddr>,
    pub draw: Option<i32>,
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
            matchmaking_type: MatchmakingType::Direct,
            matched: None,
            draw: None,
        }
    }

    pub fn set_matched(&mut self, to_update: SocketAddr) {
        self.matched = Some(to_update);
    }

    pub fn is_direct(&self) -> bool {
        match self.matchmaking_type {
            MatchmakingType::Direct => true,
            _ => false,
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
    async fn register_direct_async(
        &mut self,
        private_ip: SocketAddr,
        public_ip: SocketAddr,
    ) -> (String, bool);
    async fn match_with_direct_code_async(
        &mut self,
        private_ip: SocketAddr,
        code: String,
    ) -> (Option<SocketAddr>, i32);
    async fn get_client(&self, addr: SocketAddr) -> Option<Client>;
    async fn remove_client(&mut self, addr: SocketAddr) -> Option<Client>;
}

#[async_trait]
impl ClientsExts for Clients {
    async fn register_direct_async(
        &mut self,
        private_ip: SocketAddr,
        public_ip: SocketAddr,
    ) -> (String, bool) {
        let mut clients = self.lock().await;

        if clients.contains_key(&private_ip) {
            error!("addr {}  already registered", private_ip);
            return ("".to_string(), false);
        }

        if clients
            .iter()
            .any(|(k, _)| k.ip() == private_ip.ip() && private_ip.ip().to_string() != "127.0.0.1")
        {
            error!("ip {} already registered", private_ip.ip());
            return ("".to_string(), false);
        }

        //generate random code for direct connection
        let mut code = rand::thread_rng()
            .sample_iter(&rand::distributions::Uniform::from(b'A'..=b'Z'))
            .take(5)
            .map(char::from)
            .collect::<String>();

        while clients.values().any(|client| client.code == code) {
            code = rand::thread_rng()
                .sample_iter(&rand::distributions::Uniform::from(b'A'..=b'Z'))
                .take(5)
                .map(char::from)
                .collect::<String>();
        }

        clients.insert(private_ip, Client::new_direct(code.clone(), public_ip));
        (code, true)
    }

    async fn match_with_direct_code_async(
        &mut self,
        private_ip: SocketAddr,
        code: String,
    ) -> (Option<SocketAddr>, i32) {
        let mut clients = self.lock().await;

        if !clients.contains_key(&private_ip) {
            error!("addr {}  not registered for direct", private_ip);
            return (None, 0);
        }

        let cloned = clients.clone();

        let opponent = cloned
            .iter()
            .find(|(_, client)| client.code == code && client.is_direct());

        if let Some((_, client)) = opponent {
            let current = clients.get_mut(&private_ip).unwrap();
            current.set_matched(client.public_ip);

            if current.draw.is_none() && client.draw.is_none() {
                let rng = rand::thread_rng().gen_range(0..2);

                current.draw = Some(rng);
            } else {
                if client.draw.is_some() {
                    current.draw = Some((client.draw.unwrap() + 1) % 2);
                }
            }

            (Some(client.public_ip), current.draw.unwrap())
        } else {
            (None, 0)
        }
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
