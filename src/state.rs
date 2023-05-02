use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use rand::Rng;
use serde::Serialize;
use tokio::sync::Mutex;
use tracing::{debug, error};

use crate::services::geoip::{GeoipClient, Location};

pub type Clients = Arc<Mutex<HashMap<SocketAddr, Client>>>;

#[derive(Debug, Clone)]
pub struct Client {
    pub code: String,
    pub public_ip: SocketAddr,
    pub matchmaking_type: MatchmakingType,
    pub matched: Option<SocketAddr>,
    pub draw: Option<i32>,
    pub seed: i32,
    pub location: Location,
    pub possible_match: Option<SocketAddr>,
    pub is_waiting_choice: bool,
}

#[derive(Debug, Clone)]
pub enum MatchmakingType {
    Direct,
    QuickPlay,
}

impl Client {
    pub fn new_direct(code: String, public_ip: SocketAddr) -> Self {
        Self {
            code,
            public_ip,
            matchmaking_type: MatchmakingType::Direct,
            matched: None,
            draw: None,
            seed: -1,
            location: Location::default(),
            possible_match: None,
            is_waiting_choice: false,
        }
    }

    pub fn new_matchmaking(public_ip: SocketAddr) -> Self {
        Self {
            code: "".to_string(),
            public_ip,
            matchmaking_type: MatchmakingType::QuickPlay,
            matched: None,
            draw: None,
            seed: -1,
            location: Location::default(),
            possible_match: None,
            is_waiting_choice: false,
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

    pub fn is_quick_play(&self) -> bool {
        match self.matchmaking_type {
            MatchmakingType::QuickPlay => true,
            _ => false,
        }
    }

    pub fn is_near(&self, client: Client, max_distance: f64) -> bool {
        self.location.is_near(client.location, max_distance)
    }

    pub fn set_match(&mut self, addr: SocketAddr) {
        self.possible_match = Some(addr);
    }

    pub fn already_have_a_match(&self) -> bool {
        self.possible_match.is_some()
    }

    pub fn have_a_match_with(&self, addr: SocketAddr) -> bool {
        self.possible_match == Some(addr)
    }

    pub fn is_matched(&self) -> bool {
        self.matched.is_some()
    }

    pub fn match_for_quick_play(&mut self) {
        let opponent = self.possible_match.take().unwrap();

        self.matched = Some(opponent);
    }

    pub fn set_location(&mut self, location: Location) {
        self.location = location;
    }
}

#[derive(Debug, Clone)]
pub struct AppState {
    pub clients: Clients,
    geoip_client: GeoipClient,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            geoip_client: GeoipClient::new(),
        }
    }

    pub fn get_geoip_client(&self) -> GeoipClient {
        self.geoip_client.clone()
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
    ) -> (Option<SocketAddr>, i32, i32);
    async fn register_for_quick_play_async(
        &mut self,
        geoip_client: GeoipClient,
        private_ip: SocketAddr,
        public_ip: SocketAddr,
    ) -> bool;
    async fn make_match_for_quick_play_async(&mut self);
    async fn get_client(&self, addr: SocketAddr) -> Option<Client>;
    async fn remove_client(&mut self, addr: SocketAddr) -> Option<Client>;
    async fn set_client_waiting_choice(&mut self, addr: SocketAddr);
    async fn cancel_quick_play_async(&mut self, addr: SocketAddr);
    async fn accept_quick_play_async(&mut self, addr: SocketAddr)
        -> (Option<SocketAddr>, i32, i32);
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
    ) -> (Option<SocketAddr>, i32, i32) {
        let mut clients = self.lock().await;

        if !clients.contains_key(&private_ip) {
            error!("addr {}  not registered for direct", private_ip);
            return (None, 0, -1);
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

                let seed = rand::thread_rng().gen_range(0..1000);
                current.seed = seed;
            } else {
                if client.draw.is_some() {
                    current.draw = Some((client.draw.unwrap() + 1) % 2);
                    current.seed = client.seed;
                }
            }

            (Some(client.public_ip), current.draw.unwrap(), current.seed)
        } else {
            (None, 0, -1)
        }
    }

    async fn register_for_quick_play_async(
        &mut self,
        geoip_client: GeoipClient,
        private_ip: SocketAddr,
        public_ip: SocketAddr,
    ) -> bool {
        let mut clients = self.lock().await;

        if clients.contains_key(&private_ip) {
            error!("addr {}  already registered", private_ip);
            return false;
        }

        if clients
            .iter()
            .any(|(k, _)| k.ip() == private_ip.ip() && private_ip.ip().to_string() != "127.0.0.1")
        {
            error!("ip {} already registered", private_ip.ip());
            return false;
        }

        let mut client = Client::new_matchmaking(public_ip);

        let geoip = geoip_client.get_location(public_ip.ip()).await;

        match geoip {
            Ok(location) => {
                client.set_location(location);
                clients.insert(private_ip, client);
            }
            Err(err) => {
                error!("error when registering for matchmaking: {}", err);
                return false;
            }
        }

        true
    }

    async fn make_match_for_quick_play_async(&mut self) {
        let mut clients = self.lock().await;

        let clients_cloned = clients.clone();

        let mut matchs: HashMap<SocketAddr, SocketAddr> = HashMap::new();

        clients
            .iter_mut()
            .filter(|(_, client)| client.is_quick_play())
            .for_each(|(private_ip, client)| {
                if client.already_have_a_match() {
                    debug!("client {} already matched!", client.public_ip);
                    return;
                }

                if let Some((match1, _)) = matchs.iter().find(|(to_check1, to_check2)| {
                    *(*to_check2) == client.public_ip || *(*to_check1) == client.public_ip
                }) {
                    client.set_match(*match1);
                    return;
                }

                if let Some((_, client_matched)) = clients_cloned
                    .iter()
                    .filter(|(ip, _)| (*ip) != private_ip) //TODO: use .ip()
                    .find(|(_, to_check)| client.is_near((*to_check).clone(), 50000000.0))
                {
                    client.set_match(client_matched.public_ip);
                    matchs.insert(client.public_ip, client_matched.public_ip);
                } else {
                    debug!("client {} have no matchs", client.public_ip);
                }
            });
    }

    async fn get_client(&self, addr: SocketAddr) -> Option<Client> {
        let clients = self.lock().await;
        clients.get(&addr).cloned()
    }

    async fn remove_client(&mut self, addr: SocketAddr) -> Option<Client> {
        let mut clients = self.lock().await;
        clients.remove(&addr)
    }

    async fn set_client_waiting_choice(&mut self, public_ip: SocketAddr) {
        let mut clients = self.lock().await;

        if let Some(client) = clients.get_mut(&public_ip) {
            client.is_waiting_choice = true;
        }
    }

    async fn cancel_quick_play_async(&mut self, public_ip: SocketAddr) {
        let mut clients = self.lock().await;

        if let Some(client) = clients.get_mut(&public_ip) {
            client.is_waiting_choice = false;
        }

        clients
            .iter_mut()
            .find(|(_, client)| client.have_a_match_with(public_ip))
            .map(|(_, client)| {
                client.possible_match = None;
                client.is_waiting_choice = false;
            });

        clients.retain(|_, client| client.public_ip != public_ip);
    }

    async fn accept_quick_play_async(
        &mut self,
        private_ip: SocketAddr,
    ) -> (Option<SocketAddr>, i32, i32) {
        let mut clients = self.lock().await;

        if clients.contains_key(&private_ip) {
            let clone = clients.clone();

            let current = clients.get_mut(&private_ip).unwrap();

            let opponent_addr = current.possible_match.take().unwrap();

            let opponent = clone
                .iter()
                .find(|(_, client)| client.public_ip == opponent_addr);

            if let Some((_, opponent_client)) = opponent {
                current.set_matched(opponent_client.public_ip);
                current.is_waiting_choice = false;

                if current.draw.is_none() && opponent_client.draw.is_none() {
                    let rng = rand::thread_rng().gen_range(0..2);

                    current.draw = Some(rng);

                    let seed = rand::thread_rng().gen();
                    current.seed = seed;
                } else {
                    if opponent_client.draw.is_some() {
                        current.draw = Some((opponent_client.draw.unwrap() + 1) % 2);
                        current.seed = opponent_client.seed;
                    }
                }

                return (
                    Some(opponent_client.public_ip),
                    current.draw.unwrap(),
                    current.seed,
                );
            } else {
                error!("opponent not found");
                return (None, 0, -1);
            }
        } else {
            error!("addr {}  not registered", private_ip);
            return (None, 0, -1);
        }
    }
}
