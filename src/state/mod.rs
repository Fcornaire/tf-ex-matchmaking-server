pub mod broadcast;
pub mod client;
pub mod codes;
pub mod end_game;
pub mod lobby;
pub mod quickplay;
pub mod rating;
pub mod series;
pub mod signaling_state;

pub use quickplay::{should_auto_start, QuickPlayOutcome, WIDER_SET_MOD_NAME};

use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{atomic::AtomicU64, Arc},
    time::{Duration, Instant},
};

use axum_typed_websockets::Message;
use metrics::{counter, gauge, histogram};
use rand::Rng;
use tokio::sync::{mpsc::Sender, Mutex};
use tracing::info;

use crate::{
    msg::ServerMsg,
    services::{
        geoip::{GeoipClient, Location},
        storage::{RatingConfig, Storage},
    },
    state::lobby::{LobbyBuilder, LobbyKind},
    telemetry,
};

use self::{
    client::{Client, Room},
    lobby::{GameData, Lobby, Mod, Player},
    signaling_state::SignalingState,
};

pub type IpV4V6 = (SocketAddr, Option<SocketAddr>);
pub type Clients = Arc<Mutex<HashMap<IpV4V6, Client>>>;
pub type Lobbies = Arc<Mutex<Vec<Lobby>>>;

#[derive(Debug, Clone)]
pub struct QueuedPlayer {
    pub addr: SocketAddr,
    pub name: String,
    pub location: Location,
    pub is_wide: bool,
    pub queued_at: Instant,
}

#[derive(Debug, Clone)]
pub struct AppState {
    pub clients: Clients,
    geoip_client: GeoipClient,
    pub signaling: Arc<Mutex<SignalingState>>,
    pub ping_measurement_signaling: Arc<Mutex<SignalingState>>,
    lobbies: Lobbies,
    connections: Arc<Mutex<HashMap<SocketAddr, Sender<Message<ServerMsg>>>>>,
    end_game_countdowns: Arc<Mutex<HashMap<String, u64>>>,
    series_countdowns: Arc<Mutex<HashMap<String, u64>>>,
    end_game_generation: Arc<AtomicU64>,
    queue: Arc<Mutex<Vec<QueuedPlayer>>>,
    storage: Option<Arc<Storage>>,
    match_reports: Arc<Mutex<HashMap<String, rating::PendingReports>>>,
    rating_config: RatingConfig,
}

impl AppState {
    pub fn new() -> Self {
        Self::new_with_storage(None)
    }

    pub fn new_with_storage(storage: Option<Arc<Storage>>) -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            geoip_client: GeoipClient::new(),
            signaling: Arc::new(Mutex::new(SignalingState::default())),
            ping_measurement_signaling: Arc::new(Mutex::new(SignalingState::default())),
            lobbies: Arc::new(Mutex::new(vec![])),
            connections: Arc::new(Mutex::new(HashMap::new())),
            end_game_countdowns: Arc::new(Mutex::new(HashMap::new())),
            series_countdowns: Arc::new(Mutex::new(HashMap::new())),
            end_game_generation: Arc::new(AtomicU64::new(0)),
            queue: Arc::new(Mutex::new(vec![])),
            storage,
            match_reports: Arc::new(Mutex::new(HashMap::new())),
            rating_config: RatingConfig::from_env(),
        }
    }

    #[allow(dead_code)]
    pub fn storage(&self) -> Option<Arc<Storage>> {
        self.storage.clone()
    }

    pub fn lobbies(&self) -> Lobbies {
        self.lobbies.clone()
    }

    pub async fn publish_metrics_gauges(&self) {
        let (queue_wide, queue_standard) = {
            let queue = self.queue.lock().await;
            let wide = queue.iter().filter(|q| q.is_wide).count();

            (wide, queue.len() - wide)
        };

        let kinds = [
            LobbyKind::Standard,
            LobbyKind::QuickPlay,
            LobbyKind::Private,
        ];
        let mut lobby_counts = [0usize; 3];
        let mut game_counts = [0usize; 3];
        let mut player_counts = [0usize; 3];
        let mut spectators = 0usize;

        {
            let lobbies = self.lobbies.lock().await;

            for lobby in lobbies.iter() {
                let slot = kinds.iter().position(|k| *k == lobby.kind()).unwrap();

                lobby_counts[slot] += 1;
                player_counts[slot] += lobby.players().len();
                spectators += lobby.spectators().len();

                if lobby.in_game() {
                    game_counts[slot] += 1;
                }
            }
        }

        let (clients, clients_by_country) = {
            let clients = self.clients.lock().await;
            let mut by_country: HashMap<String, usize> = HashMap::new();

            for client in clients.values() {
                let country = client.location.get_country_code();
                let country = if country.is_empty() {
                    "unknown".to_string()
                } else {
                    country
                };

                *by_country.entry(country).or_default() += 1;
            }

            (clients.len(), by_country)
        };

        let (signaling_rooms, signaling_peers) = {
            let signaling = self.signaling.lock().await;

            (signaling.rooms.len(), signaling.clients.len())
        };

        gauge!(telemetry::QUICKPLAY_QUEUE_DEPTH, "wide" => "true").set(queue_wide as f64);
        gauge!(telemetry::QUICKPLAY_QUEUE_DEPTH, "wide" => "false").set(queue_standard as f64);

        for (slot, kind) in kinds.iter().enumerate() {
            let label = telemetry::kind_label(*kind);

            gauge!(telemetry::LOBBIES, "kind" => label).set(lobby_counts[slot] as f64);
            gauge!(telemetry::GAMES_ACTIVE, "kind" => label).set(game_counts[slot] as f64);
            gauge!(telemetry::PLAYERS_SEATED, "kind" => label).set(player_counts[slot] as f64);
        }

        gauge!(telemetry::SPECTATORS).set(spectators as f64);
        gauge!(telemetry::CLIENTS_CONNECTED).set(clients as f64);
        telemetry::publish_clients_by_country(clients_by_country);
        gauge!(telemetry::SIGNALING_ROOMS).set(signaling_rooms as f64);
        gauge!(telemetry::SIGNALING_PEERS).set(signaling_peers as f64);

        let pending_reports = self.match_reports.lock().await.len();
        gauge!(telemetry::MATCH_REPORTS_PENDING).set(pending_reports as f64);

        if let Some(storage) = &self.storage {
            match storage.top_players(20).await {
                Ok(top) => {
                    for (player_id, name, rating) in top {
                        gauge!(telemetry::PLAYER_RATING, "player_id" => player_id, "name" => name)
                            .set(rating);
                    }
                }
                Err(err) => {
                    info!("Could not publish top players : {}", err);
                }
            }

            match storage.community_stats().await {
                Ok(stats) => {
                    gauge!(telemetry::PLAYERS_TOTAL).set(stats.players_total as f64);
                    gauge!(telemetry::PLAYERS_ACTIVE, "window" => "24h")
                        .set(stats.active_24h as f64);
                    gauge!(telemetry::PLAYERS_ACTIVE, "window" => "7d").set(stats.active_7d as f64);
                    gauge!(telemetry::RATED_MATCHES_RECENT, "window" => "24h")
                        .set(stats.rated_24h as f64);
                    gauge!(telemetry::RATED_MATCHES_RECENT, "window" => "7d")
                        .set(stats.rated_7d as f64);
                }
                Err(err) => {
                    info!("Could not publish community stats : {}", err);
                }
            }
        }
    }

    pub async fn client_label(&self, addr: SocketAddr) -> String {
        self.clients
            .lock()
            .await
            .iter()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || *ipv6 == Some(addr))
            .map(|(_, client)| client.label())
            .unwrap_or_else(|| "unknown client".to_string())
    }

    pub async fn member_label(&self, addr: &str) -> String {
        match addr.parse::<SocketAddr>() {
            Ok(addr) => self.client_label(addr).await,
            Err(_) => "unknown client".to_string(),
        }
    }

    pub async fn identify_client(
        &self,
        addr: SocketAddr,
        player_id: String,
        name: String,
    ) -> Result<(), String> {
        let valid_prefix = player_id.starts_with("steam:") || player_id.starts_with("local:");

        if !valid_prefix || player_id.len() > 64 {
            return Err(format!("Invalid player id '{}'", player_id));
        }

        {
            let mut clients = self.clients.lock().await;

            let client = clients
                .iter_mut()
                .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
                .map(|(_, client)| client)
                .ok_or("Client not found".to_string())?;

            client.player_id = Some(player_id.clone());
            client.name = name.clone();
        }

        if let Some(storage) = &self.storage {
            let rating = storage.get_or_create_player(player_id, name).await?;

            info!(
                "{} ({}) identified (rating {:.0}, {} matches)",
                rating.name, rating.id, rating.rating, rating.matches_played
            );
        }

        Ok(())
    }

    pub async fn update_alive_client(&self, addr: SocketAddr) -> Result<(), String> {
        let mut clients = self.clients.lock().await;

        let client_res = clients
            .iter_mut()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr);

        if client_res.is_none() {
            return Err("Client not found".to_string());
        }

        let client = client_res.unwrap().1;

        if client.is_alive() {
            client.alive();
        }

        Ok(())
    }

    pub async fn record_keep_alive_rtt(&self, addr: SocketAddr) {
        let mut clients = self.clients.lock().await;

        let rtt = clients
            .iter_mut()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
            .and_then(|(_, client)| client.take_keep_alive_rtt());

        if let Some(rtt) = rtt {
            histogram!(telemetry::KEEPALIVE_RTT_SECONDS).record(rtt as f64 / 1000.0);
        }
    }

    pub async fn add_connexion(
        &self,
        addr: SocketAddr,
        sender: Sender<Message<ServerMsg>>,
    ) -> Result<(), String> {
        let mut connexions = self.connections.lock().await;

        if connexions.contains_key(&addr) {
            return Err("Connexion already exists".to_string());
        }

        connexions.insert(addr, sender);

        Ok(())
    }

    pub async fn remove_connexion(&self, addr: SocketAddr) -> Result<(), String> {
        let mut connexions = self.connections.lock().await;

        if !connexions.contains_key(&addr) {
            return Err("Connexion not found".to_string());
        }

        connexions.remove(&addr);

        Ok(())
    }

    pub async fn add_new_client(&self, addrs: Vec<SocketAddr>) -> Result<(), String> {
        let ipv4 = addrs[0];

        let location = self.geoip_client.get_location(ipv4.ip()).await;

        if location.is_err() {
            counter!(telemetry::GEOIP_FAILURES_TOTAL).increment(1);
            return Err("Could not get location".to_string());
        }

        let client = Client::new(location.unwrap());

        if addrs.len() == 2 {
            let ipv6 = addrs[1];
            self.clients.lock().await.insert((ipv4, Some(ipv6)), client);
            return Ok(());
        }

        self.clients.lock().await.insert((ipv4, None), client);

        Ok(())
    }

    pub fn get_geoip_client(&self) -> GeoipClient {
        self.geoip_client.clone()
    }

    pub async fn get_lobbies(&self, _addr: SocketAddr) -> Vec<Lobby> {
        let mut lobbies: Vec<Lobby> = self
            .lobbies
            .lock()
            .await
            .iter()
            .filter(|l| l.players().iter().any(|p| p.is_host()))
            .filter(|l| match l.kind() {
                LobbyKind::Standard => !l.is_full() || l.in_game() || l.series_in_progress(),
                LobbyKind::QuickPlay => l.in_game(),
                LobbyKind::Private => false,
            })
            .cloned()
            .collect();

        lobbies.sort_by_key(|l| l.in_game());

        lobbies
    }

    pub async fn add_lobby(&self, lobby: &mut Lobby, addr: SocketAddr) -> Result<Lobby, String> {
        let mut lobbies = self.lobbies.lock().await;

        if lobbies.iter().any(|l| l.room_id() == lobby.room_id()) {
            return Err("Lobby with that room id already exists".to_string());
        }

        let mut clients = self.clients.lock().await;
        let client = clients
            .iter_mut()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
            .unwrap()
            .1;

        client.room = Some(Room::new(lobby.room_id().clone()));

        let lobb = lobby.clone();
        let host_res: Option<Player> = lobb.players().first().cloned();

        if host_res.is_none() {
            return Err("Host not found".to_string());
        }

        let mut host = host_res.unwrap().clone();
        client.name = host.name();

        host.update_addr(addr.to_string());
        host.update_room_peer_id(client.peer_id(lobby.room_id().clone()).unwrap());
        host.set_seat(0);

        lobby.update_seed(rand::thread_rng().gen_range(0..100000));

        let join_code = match lobby.kind() {
            LobbyKind::Private => {
                let taken: HashSet<String> = lobbies.iter().filter_map(|l| l.join_code()).collect();

                Some(codes::generate_join_code(&taken))
            }
            _ => None,
        };

        let final_lobby = LobbyBuilder::new()
            .name(lobby.name())
            .room_id(lobby.room_id())
            .max_players(lobby.max_players())
            .game_data(lobby.game_data())
            .players(vec![host])
            .mods(lobby.mods())
            .kind(lobby.kind())
            .join_code(join_code)
            .build();

        lobbies.push(final_lobby.clone());

        counter!(telemetry::LOBBIES_CREATED_TOTAL, "kind" => telemetry::kind_label(final_lobby.kind()))
            .increment(1);

        Ok(final_lobby)
    }

    pub async fn join_lobby(
        &self,
        room_id: String,
        addr: SocketAddr,
        name: String,
        is_player: bool,
    ) -> Result<(Lobby, String), String> {
        let mut lobbies = self.lobbies.lock().await;

        Self::guard_not_in_lobby(&lobbies, addr)?;

        let lobby = lobbies
            .iter_mut()
            .find(|l| l.room_id() == room_id)
            .ok_or(format!("Lobby {} not found", room_id))?;

        let room_peer_id = self.seat_member(lobby, addr, name, is_player).await?;

        Ok((lobby.clone(), room_peer_id))
    }

    pub async fn join_private(
        &self,
        code: String,
        addr: SocketAddr,
        name: String,
        is_player: bool,
    ) -> Result<(Lobby, String), String> {
        {
            let mut clients = self.clients.lock().await;

            let client = clients
                .iter_mut()
                .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
                .ok_or("Client not found".to_string())?
                .1;

            if let Some(last) = client.last_private_join_attempt {
                if last.elapsed()
                    < Duration::from_secs(crate::constants::PRIVATE_JOIN_COOLDOWN_SECS)
                {
                    counter!(telemetry::PRIVATE_JOIN_FAILURES_TOTAL, "reason" => "rate_limited")
                        .increment(1);
                    return Err("Too many attempts, wait a moment".to_string());
                }
            }

            client.last_private_join_attempt = Some(std::time::Instant::now());
        }

        let code = codes::normalize_code(&code);

        let mut lobbies = self.lobbies.lock().await;

        Self::guard_not_in_lobby(&lobbies, addr)?;

        let lobby = lobbies
            .iter_mut()
            .find(|l| l.join_code().as_deref() == Some(code.as_str()))
            .ok_or_else(|| {
                counter!(telemetry::PRIVATE_JOIN_FAILURES_TOTAL, "reason" => "invalid_code")
                    .increment(1);

                "Invalid or expired code".to_string()
            })?;

        let room_peer_id = self.seat_member(lobby, addr, name, is_player).await?;

        Ok((lobby.clone(), room_peer_id))
    }

    fn guard_not_in_lobby(lobbies: &[Lobby], addr: SocketAddr) -> Result<(), String> {
        let exist = lobbies
            .iter()
            .find(|l| l.players().iter().any(|p| p.addr() == addr.to_string()));

        if exist.is_some() {
            return Err("Player already in a lobby".to_string());
        }

        Ok(())
    }

    pub(super) async fn seat_member(
        &self,
        lobby: &mut Lobby,
        addr: SocketAddr,
        name: String,
        is_player: bool,
    ) -> Result<String, String> {
        let room_id = lobby.room_id();

        if is_player {
            if lobby.is_full() {
                return Err(format!("Lobby {} is full", room_id));
            }

            if lobby.in_game() {
                return Err(format!("Lobby {} already started", room_id));
            }

            if lobby.series_in_progress() {
                return Err(format!("Lobby {} has a series running", room_id));
            }
        }

        let mut clients: tokio::sync::MutexGuard<'_, HashMap<IpV4V6, Client>> =
            self.clients.lock().await;

        let client_res = clients
            .iter_mut()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr);

        if client_res.is_none() {
            return Err("Client not found".to_string());
        }

        let client = client_res.unwrap().1;

        client.room = Some(Room::new(room_id.clone()));
        client.name = name.clone();

        let room_peer_id = client
            .peer_id(room_id)
            .ok_or("Couldn't retrieve Room peer id")?;

        let member = Player::new(
            client.name.clone(),
            addr.to_string(),
            room_peer_id.clone(),
            false,
        );

        if is_player {
            lobby.add_player(member);
        } else {
            lobby.add_spectator(member);
        }

        counter!(telemetry::JOINS_TOTAL, "role" => if is_player { "player" } else { "spectator" })
            .increment(1);

        Ok(room_peer_id)
    }

    pub async fn remove_from_lobby(&self, addr: SocketAddr) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|lobby| {
                lobby.players().iter().any(|p| p.addr() == addr.to_string())
                    || lobby
                        .spectators()
                        .iter()
                        .any(|p| p.addr() == addr.to_string())
            })
            .ok_or("Lobby not found".to_string())?;

        let mut clients = self.clients.lock().await;

        let client_res = clients
            .iter_mut()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr);

        if client_res.is_none() {
            return Err("Client not found".to_string());
        }

        let client: &mut Client = client_res.unwrap().1;

        client.room = None;

        let series_running = lobby.series_in_progress();
        lobby.remove_player_or_spectator(addr.to_string());
        lobby.promote_host_if_needed();

        if series_running && !lobby.series_in_progress() {
            info!("Series in room {} aborted, a player left", lobby.room_id());
            counter!(telemetry::SERIES_OUTCOMES_TOTAL, "outcome" => "aborted").increment(1);
        }

        Ok(lobby.room_id())
    }

    pub async fn start_lobby(&self, addr: SocketAddr) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|lobby| lobby.players().iter().any(|p| p.addr() == addr.to_string()))
            .ok_or("Lobby not found".to_string())?;

        let is_host = lobby
            .players()
            .iter()
            .any(|p| p.addr() == addr.to_string() && p.is_host());

        if !is_host {
            return Err("Only the host can start the lobby".to_string());
        }

        if lobby.in_game() {
            return Err("Lobby already started".to_string());
        }

        let series_started = if lobby.is_series_lobby() {
            if lobby.series_in_progress() {
                return Err("Series already running".to_string());
            }

            lobby.start_series()?;
            lobby
                .begin_series_game()
                .ok_or("No tower available for the series".to_string())?;
            true
        } else {
            false
        };

        if lobby.end_game_choice().is_none() {
            lobby.initialize_end_game_choice();
        }

        lobby.set_in_game(true);

        counter!(telemetry::MATCHES_STARTED_TOTAL, "kind" => telemetry::kind_label(lobby.kind()), "trigger" => "manual")
            .increment(1);

        let lobby = lobby.clone();
        drop(lobbies);

        if series_started {
            counter!(telemetry::SERIES_OUTCOMES_TOTAL, "outcome" => "started").increment(1);

            self.send_msg_to_lobby(
                lobby.clone(),
                ServerMsg::LobbyUpdate {
                    lobby: lobby.clone(),
                },
            )
            .await;
        }

        self.send_msg_to_lobby(lobby.clone(), ServerMsg::StartLobby {})
            .await;

        Ok(lobby.room_id())
    }

    pub async fn rematch_lobby(&self, addr: SocketAddr) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|lobby| lobby.players().iter().any(|p| p.addr() == addr.to_string()))
            .ok_or("Lobby not found".to_string())?;

        lobby.rematch(addr.to_string());

        Ok(lobby.room_id())
    }

    pub async fn archer_select_choice_lobby(&self, addr: SocketAddr) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|lobby| lobby.players().iter().any(|p| p.addr() == addr.to_string()))
            .ok_or("Lobby not found".to_string())?;

        lobby.archer_select_choice(addr.to_string());

        Ok(lobby.room_id())
    }

    pub async fn room_id_for(&self, addr: SocketAddr) -> Option<String> {
        let lobbies = self.lobbies.lock().await;

        lobbies
            .iter()
            .find(|lobby| lobby.players().iter().any(|p| p.addr() == addr.to_string()))
            .map(|lobby| lobby.room_id())
    }

    pub async fn purge_lobby_async(&self) {
        let mut lobbies = self.lobbies.lock().await;

        let count = lobbies.len();
        lobbies.retain(|l| !l.players().is_empty());
        if count != lobbies.len() {
            info!("Purged {} empty lobbies", count - lobbies.len());
        }

        let count = lobbies.len();
        lobbies.retain(|l| l.players().iter().any(|p| p.is_host()));

        if count != lobbies.len() {
            info!("Purged {} lobbies with no host", count - lobbies.len());
        }
    }

    pub async fn update_player(&self, addr: SocketAddr, player: Player) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let addr = addr.to_string();
        let lobby = lobbies
            .iter_mut()
            .find(|l| l.players().iter().any(|p| p.addr() == addr))
            .ok_or("Lobby not found".to_string())?;

        lobby.update_player(&addr, player);

        Ok(lobby.room_id())
    }

    pub async fn update_lobby_settings(
        &self,
        addr: SocketAddr,
        max_players: usize,
        game_data: GameData,
        mods: Vec<Mod>,
    ) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|l| l.players().iter().any(|p| p.addr() == addr.to_string()))
            .ok_or("Lobby not found".to_string())?;

        if !lobby
            .players()
            .iter()
            .any(|p| p.addr() == addr.to_string() && p.is_host())
        {
            return Err("Only the host can update lobby settings".to_string());
        }

        if lobby.in_game() {
            return Err("Cannot update lobby settings while in game".to_string());
        }

        if lobby.series_in_progress() {
            return Err("Cannot update lobby settings during a series".to_string());
        }

        lobby.update_settings(max_players, game_data, mods);

        Ok(lobby.room_id())
    }

    pub async fn remove_all_empty_signaling_rooms(&self) {
        for signaling in [&self.signaling, &self.ping_measurement_signaling] {
            let mut signaling = signaling.lock().await;

            let mut rooms_to_remove = vec![];

            for (room, peers) in signaling.rooms.iter() {
                if peers.is_empty() {
                    rooms_to_remove.push(room.clone());
                }
            }

            for room in rooms_to_remove.clone() {
                signaling.rooms.remove(&room);
            }

            if rooms_to_remove.len() > 0 {
                info!("Removed {} empty signaling rooms", rooms_to_remove.len());
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::net::SocketAddr;

    use crate::services::geoip::Location;

    use super::{
        client::Client,
        lobby::{GameData, Lobby, LobbyBuilder, LobbyKind, Player},
        AppState,
    };

    pub fn test_addr(n: u8) -> SocketAddr {
        format!("127.0.0.{}:4000", n).parse().unwrap()
    }

    pub async fn state_with_clients(addrs: &[SocketAddr]) -> AppState {
        let state = AppState::new();

        for addr in addrs {
            state
                .clients
                .lock()
                .await
                .insert((*addr, None), Client::new(Location::default()));
        }

        state
    }

    pub fn lobby_payload(kind: LobbyKind, room_id: &str) -> Lobby {
        LobbyBuilder::new()
            .name("tester".to_string())
            .room_id(room_id.to_string())
            .game_data(GameData {
                map_id: -1,
                mode: 3,
                match_length: 2,
                variants: vec![],
                seed: 0,
                best_of: 0,
            })
            .players(vec![Player::new(
                "tester".to_string(),
                "".to_string(),
                "".to_string(),
                true,
            )])
            .kind(kind)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;

    #[tokio::test]
    async fn private_lobby_gets_code_and_hides_from_browser() {
        let host = test_addr(1);
        let state = state_with_clients(&[host]).await;

        let mut payload = lobby_payload(LobbyKind::Private, "room-1");
        let created = state.add_lobby(&mut payload, host).await.unwrap();

        assert_eq!(created.kind(), LobbyKind::Private);
        assert_eq!(created.join_code().unwrap().len(), codes::CODE_LEN);
        assert!(state.get_lobbies(host).await.is_empty());
    }

    #[tokio::test]
    async fn join_private_normalizes_code_and_rejects_bad_one() {
        let host = test_addr(1);
        let joiner = test_addr(2);
        let state = state_with_clients(&[host, joiner]).await;

        let mut payload = lobby_payload(LobbyKind::Private, "room-1");
        let created = state.add_lobby(&mut payload, host).await.unwrap();
        let code = created.join_code().unwrap();

        let err = state
            .join_private("NOPE".to_string(), joiner, "guest".to_string(), true)
            .await
            .unwrap_err();
        assert_eq!(err, "Invalid or expired code");

        let throttled = state
            .join_private(code.clone(), joiner, "guest".to_string(), true)
            .await
            .unwrap_err();
        assert_eq!(throttled, "Too many attempts, wait a moment");

        state
            .clients
            .lock()
            .await
            .iter_mut()
            .for_each(|(_, client)| client.last_private_join_attempt = None);

        let sloppy = format!(" {} ", code.to_lowercase());
        let (lobby, room_peer_id) = state
            .join_private(sloppy, joiner, "guest".to_string(), true)
            .await
            .unwrap();

        assert!(!room_peer_id.is_empty());
        assert_eq!(lobby.players().len(), 2);
    }

    #[tokio::test]
    async fn in_game_lobby_rejects_players_but_accepts_spectators_and_stays_listed() {
        let host = test_addr(1);
        let joiner = test_addr(2);
        let watcher = test_addr(3);
        let qp_host = test_addr(4);
        let state = state_with_clients(&[host, joiner, watcher, qp_host]).await;

        let mut payload = lobby_payload(LobbyKind::Standard, "room-1");
        state.add_lobby(&mut payload, host).await.unwrap();
        state.lobbies.lock().await[0].set_in_game(true);

        let err = state
            .join_lobby("room-1".to_string(), joiner, "guest".to_string(), true)
            .await
            .unwrap_err();
        assert_eq!(err, "Lobby room-1 already started");

        let (_, room_peer_id) = state
            .join_lobby("room-1".to_string(), watcher, "watcher".to_string(), false)
            .await
            .unwrap();
        assert!(!room_peer_id.is_empty());

        let lobbies = state.get_lobbies(host).await;
        assert_eq!(lobbies.len(), 1);
        assert_eq!(lobbies[0].spectators().len(), 1);

        let mut qp_payload = lobby_payload(LobbyKind::QuickPlay, "room-qp");
        state.add_lobby(&mut qp_payload, qp_host).await.unwrap();
        assert_eq!(state.get_lobbies(host).await.len(), 1);

        state
            .lobbies
            .lock()
            .await
            .iter_mut()
            .find(|l| l.room_id() == "room-qp")
            .unwrap()
            .set_in_game(true);
        assert_eq!(state.get_lobbies(host).await.len(), 2);
    }

    #[tokio::test]
    async fn standard_lobby_stays_listed_without_code() {
        let host = test_addr(1);
        let state = state_with_clients(&[host]).await;

        let mut payload = lobby_payload(LobbyKind::Standard, "room-1");
        let created = state.add_lobby(&mut payload, host).await.unwrap();

        assert!(created.join_code().is_none());
        assert_eq!(state.get_lobbies(host).await.len(), 1);
    }
}
