use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use rand::Rng;
use time::{macros::format_description, OffsetDateTime};
use tokio::sync::Mutex;
use tracing::{debug, error};

use crate::{
    constants::{MAX_MATCH_DISTANCE, MIN_MATCH_DISTANCE},
    services::geoip::{GeoipClient, Location},
};

pub type Clients = Arc<Mutex<HashMap<SocketAddr, Client>>>;

#[derive(Debug, Clone)]
pub struct Client {
    pub name: String,
    pub code: String,
    pub public_ip: SocketAddr,
    pub matchmaking_type: MatchmakingType,
    pub matched: Option<SocketAddr>,
    pub draw: Option<i32>,
    pub seed: i32,
    pub location: Location,
    pub possible_match: Option<SocketAddr>,
    pub is_waiting_choice: bool,
    pub matched_history: HashMap<SocketAddr, String>,
    pub max_match_distance: Option<f64>,
}

#[derive(Debug, Clone)]
pub enum MatchmakingType {
    Direct,
    QuickPlay,
}

impl Client {
    pub fn new_direct(code: String, public_ip: SocketAddr, name: String) -> Self {
        Self {
            name,
            code,
            public_ip,
            matchmaking_type: MatchmakingType::Direct,
            matched: None,
            draw: None,
            seed: -1,
            location: Location::default(),
            possible_match: None,
            is_waiting_choice: false,
            matched_history: HashMap::new(),
            max_match_distance: None,
        }
    }

    pub fn new_quickplay(public_ip: SocketAddr, name: String) -> Self {
        Self {
            name,
            code: "".to_string(),
            public_ip,
            matchmaking_type: MatchmakingType::QuickPlay,
            matched: None,
            draw: None,
            seed: -1,
            location: Location::default(),
            possible_match: None,
            is_waiting_choice: false,
            matched_history: HashMap::new(),
            max_match_distance: Some(MIN_MATCH_DISTANCE),
        }
    }

    pub fn extend_max_match_distance(&mut self) {
        if let Some(max_distance) = self.max_match_distance {
            self.max_match_distance =
                Some((max_distance + MIN_MATCH_DISTANCE).min(MAX_MATCH_DISTANCE));
        }
    }

    pub fn match_max_distance(&self) -> f64 {
        if let Some(max_distance) = self.max_match_distance {
            return max_distance;
        }
        MAX_MATCH_DISTANCE
    }

    pub fn add_match_history(&mut self, opponent: SocketAddr) {
        if !self.matched_history.contains_key(&opponent) {
            let format = format_description!(
                "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond] [offset_hour sign:mandatory]:[offset_minute]:[offset_second]"
            );

            self.matched_history
                .insert(opponent, OffsetDateTime::now_utc().format(&format).unwrap());
        }
    }

    pub fn already_matched_with(&self, opponent: SocketAddr) -> bool {
        self.matched_history.contains_key(&opponent)
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

    pub fn is_matched_to(&self, addr: SocketAddr) -> bool {
        self.is_matched() && self.matched == Some(addr)
    }

    pub fn match_for_quick_play(&mut self) {
        let opponent = self.possible_match.take().unwrap();

        self.matched = Some(opponent);
    }

    pub fn set_location(&mut self, location: Location) {
        self.location = location;
    }

    fn remove_match_history(&mut self, public_ip: &SocketAddr) {
        self.matched_history.remove(public_ip);
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
        name: String,
    ) -> (String, bool);
    async fn match_with_direct_code_async(
        &mut self,
        private_ip: SocketAddr,
        code: String,
    ) -> (Option<SocketAddr>, i32, i32, String);
    async fn register_for_quick_play_async(
        &mut self,
        geoip_client: GeoipClient,
        private_ip: SocketAddr,
        public_ip: SocketAddr,
        name: String,
    ) -> bool;
    async fn make_match_for_quick_play_async(&mut self) -> i32;
    async fn get_client(&self, addr: SocketAddr) -> Option<Client>;
    async fn get_client_by_public_ip(&self, addr: SocketAddr) -> Option<Client>;
    async fn remove_client(&mut self, addr: SocketAddr) -> Option<Client>;
    async fn set_client_waiting_choice(&mut self, addr: SocketAddr);
    async fn cancel_quick_play_async(&mut self, addr: SocketAddr);
    async fn accept_quick_play_async(
        &mut self,
        addr: SocketAddr,
    ) -> (Option<SocketAddr>, i32, i32, String);
    async fn update_matched_history_async(&mut self, max_duration: Duration) -> i32;
    async fn get_total_matchable_clients_in_quick_play_async(&self) -> i32;
}

#[async_trait]
impl ClientsExts for Clients {
    async fn register_direct_async(
        &mut self,
        private_ip: SocketAddr,
        public_ip: SocketAddr,
        name: String,
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

        clients.insert(
            private_ip,
            Client::new_direct(code.clone(), public_ip, name),
        );
        (code, true)
    }

    async fn match_with_direct_code_async(
        &mut self,
        private_ip: SocketAddr,
        code: String,
    ) -> (Option<SocketAddr>, i32, i32, String) {
        let mut clients = self.lock().await;

        if !clients.contains_key(&private_ip) {
            error!("addr {}  not registered for direct", private_ip);
            return (None, 0, -1, "".to_string());
        }

        let cloned = clients.clone();

        let opponent = cloned
            .iter()
            .find(|(_, client)| client.code == code && client.is_direct());

        if let Some((_, opponent_client)) = opponent {
            let current = clients.get_mut(&private_ip).unwrap();
            current.set_matched(opponent_client.public_ip);

            if current.draw.is_none() && opponent_client.draw.is_none() {
                let rng = rand::thread_rng().gen_range(0..2);

                current.draw = Some(rng);

                let seed = rand::thread_rng().gen_range(0..100000);
                current.seed = seed;
            } else {
                if opponent_client.draw.is_some() {
                    current.draw = Some((opponent_client.draw.unwrap() + 1) % 2);
                    current.seed = opponent_client.seed;
                }
            }

            (
                Some(opponent_client.public_ip),
                current.draw.unwrap(),
                current.seed,
                opponent_client.name.clone(),
            )
        } else {
            (None, 0, -1, "".to_string())
        }
    }

    async fn register_for_quick_play_async(
        &mut self,
        geoip_client: GeoipClient,
        private_ip: SocketAddr,
        public_ip: SocketAddr,
        name: String,
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

        let mut client = Client::new_quickplay(public_ip, name);

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

    async fn make_match_for_quick_play_async(&mut self) -> i32 {
        let mut clients = self.lock().await;

        let clients_cloned = clients.clone();

        let mut matchs: HashMap<SocketAddr, SocketAddr> = HashMap::new();
        let mut count = 0;

        clients
            .iter_mut()
            .filter(|(_, client)| {
                client.is_quick_play() && !client.is_matched() && !client.already_have_a_match()
            })
            .for_each(|(private_ip, client)| {
                if let Some((match1, _)) = matchs.iter().find(|(to_check1, to_check2)| {
                    *(*to_check2) == client.public_ip || *(*to_check1) == client.public_ip
                }) {
                    if !client.already_matched_with(*match1) {
                        client.set_match(*match1);
                        client.add_match_history(*match1);
                    }
                    return;
                }

                if let Some((_, client_matched)) = clients_cloned
                    .iter()
                    .filter(|(ip, _)| (*ip) != private_ip) //TODO: use .ip()
                    .filter(|(_, potential_client)| {
                        !potential_client.already_matched_with(client.public_ip)
                    })
                    .filter(|(_, to_check)| {
                        client.is_near((*to_check).clone(), client.match_max_distance())
                    })
                    .min_by_key(|(_, potential_client)| {
                        potential_client
                            .location
                            .distance_to(client.location.clone()) as i64
                    })
                {
                    client.set_match(client_matched.public_ip);
                    client.add_match_history(client_matched.public_ip);
                    matchs.insert(client.public_ip, client_matched.public_ip);
                    count += 1;
                } else {
                    debug!(
                        "no match found for {} with max distance {}",
                        client.public_ip,
                        client.match_max_distance()
                    );
                    client.extend_max_match_distance();
                }
            });

        count
    }

    async fn update_matched_history_async(&mut self, max_duration: Duration) -> i32 {
        let mut clients = self.lock().await;

        let duration = time::Duration::new(max_duration.as_secs().try_into().unwrap(), 0);
        let format = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond] [offset_hour sign:mandatory]:[offset_minute]:[offset_second]"
        );
        let now = OffsetDateTime::now_utc();
        let mut count = 0;

        clients.iter_mut().for_each(|(_, client)| {
            let matched_history = client.matched_history.clone();

            matched_history.iter().for_each(|(public_ip, time)| {
                let date = OffsetDateTime::parse(time, &format).unwrap();

                match date.checked_add(duration) {
                    Some(date) => {
                        if date <= now {
                            client.remove_match_history(public_ip);
                            count += 1;
                        }
                    }
                    None => {}
                };
            })
        });

        count
    }

    async fn get_client(&self, addr: SocketAddr) -> Option<Client> {
        let clients = self.lock().await;
        clients.get(&addr).cloned()
    }

    async fn get_client_by_public_ip(&self, addr: SocketAddr) -> Option<Client> {
        let clients = self.lock().await;
        clients
            .iter()
            .find(|(_, client)| client.public_ip == addr)
            .map(|(_, client)| client.clone())
    }

    async fn remove_client(&mut self, addr: SocketAddr) -> Option<Client> {
        let mut clients = self.lock().await;
        clients.remove(&addr)
    }

    async fn set_client_waiting_choice(&mut self, private_ip: SocketAddr) {
        let mut clients = self.lock().await;

        if let Some(client) = clients.get_mut(&private_ip) {
            client.is_waiting_choice = true;
        }
    }

    async fn cancel_quick_play_async(&mut self, public_ip: SocketAddr) {
        let mut clients = self.lock().await;

        if let Some((_, client)) = clients
            .iter_mut()
            .find(|(_private_ip, client)| client.public_ip == public_ip)
        {
            client.is_waiting_choice = false;
            client.matched.take();
            client.possible_match.take();
        }

        if let Some((_, client)) = clients.iter_mut().find(|(_, client)| {
            client.have_a_match_with(public_ip) || client.is_matched_to(public_ip)
        }) {
            client.possible_match.take();
            client.matched.take();
            client.is_waiting_choice = false;
        };
    }

    async fn accept_quick_play_async(
        &mut self,
        private_ip: SocketAddr,
    ) -> (Option<SocketAddr>, i32, i32, String) {
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

                    let seed = rand::thread_rng().gen_range(0..100000);
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
                    opponent_client.name.clone(),
                );
            } else {
                error!("opponent not found : {}", opponent_addr);
                return (None, 0, -1, String::from(""));
            }
        } else {
            error!("addr {}  not registered", private_ip);
            return (None, 0, -1, String::from(""));
        }
    }

    async fn get_total_matchable_clients_in_quick_play_async(&self) -> i32 {
        let clients = self.lock().await;

        clients
            .iter()
            .filter(|(_, client)| {
                client.is_quick_play() && (!client.already_have_a_match() || !client.is_matched())
            })
            .count() as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extend_max_match_distance_with_max_distance_less_than_max() {
        let mut state = Client::new_quickplay("127.0.0.1:3000".parse().unwrap(), "".to_string());
        state.max_match_distance = Some(10.0);
        state.extend_max_match_distance();
        assert_eq!(state.max_match_distance, Some(MIN_MATCH_DISTANCE + 10.0));
    }

    #[test]
    fn test_extend_max_match_distance_with_max_distance_equal_to_max() {
        let mut state = Client::new_quickplay("127.0.0.1:3000".parse().unwrap(), "".to_string());
        state.max_match_distance = Some(MAX_MATCH_DISTANCE);
        state.extend_max_match_distance();
        assert_eq!(state.max_match_distance, Some(MAX_MATCH_DISTANCE));
    }

    #[test]
    fn test_extend_max_match_distance_with_max_distance_greater_than_max() {
        let mut state = Client::new_quickplay("127.0.0.1:3000".parse().unwrap(), "".to_string());
        state.max_match_distance = Some(MAX_MATCH_DISTANCE + 10.0);
        state.extend_max_match_distance();
        assert_eq!(state.max_match_distance, Some(MAX_MATCH_DISTANCE));
    }
}
