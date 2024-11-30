pub mod client;
pub mod lobby;
pub mod signaling_state;

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum_typed_websockets::Message;
use rand::Rng;
use tokio::sync::{mpsc::Sender, Mutex};
use tracing::info;

use crate::{
    msg::ServerMsg,
    services::geoip::GeoipClient,
    state::lobby::{EndGameChoice, LobbyBuilder},
};

use self::{
    client::{Client, Room},
    lobby::{Lobby, Player},
    signaling_state::SignalingState,
};

pub type IpV4V6 = (SocketAddr, Option<SocketAddr>);
pub type Clients = Arc<Mutex<HashMap<IpV4V6, Client>>>;
pub type Lobbies = Arc<Mutex<Vec<Lobby>>>;

#[derive(Debug, Clone)]
pub struct AppState {
    pub clients: Clients,
    geoip_client: GeoipClient,
    pub signaling: Arc<Mutex<SignalingState>>,
    lobbies: Lobbies,
    connections: Arc<Mutex<HashMap<SocketAddr, Sender<Message<ServerMsg>>>>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            clients: Arc::new(Mutex::new(HashMap::new())),
            geoip_client: GeoipClient::new(),
            signaling: Arc::new(Mutex::new(SignalingState::default())),
            lobbies: Arc::new(Mutex::new(vec![])),
            connections: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn lobbies(&self) -> Lobbies {
        self.lobbies.clone()
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
        let lobbies: Vec<Lobby> = self
            .lobbies
            .lock()
            .await
            .iter()
            .filter(|l| !l.is_full())
            .cloned()
            .collect();

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

        client.name = lobby.name();
        client.room = Some(Room::new(lobby.room_id().clone()));
        client.room_chat = Some(Room::new(lobby.room_chat_id().clone()));

        let lobb = lobby.clone();
        let host_res: Option<Player> = lobb.players().first().cloned();

        if host_res.is_none() {
            return Err("Host not found".to_string());
        }

        let mut host = host_res.unwrap().clone();

        host.update_addr(addr.to_string());
        host.update_room_peer_id(client.peer_id(lobby.room_id().clone()).unwrap());
        host.update_room_chat_peer_id(client.peer_id(lobby.room_chat_id().clone()).unwrap());

        lobby.update_seed(rand::thread_rng().gen_range(0..100000));

        let final_lobby = LobbyBuilder::new()
            .name(lobby.name())
            .room_id(lobby.room_id())
            .room_chat_id(lobby.room_chat_id())
            .game_data(lobby.game_data())
            .players(vec![host])
            .build();

        lobbies.push(final_lobby.clone());

        Ok(final_lobby)
    }

    pub async fn join_lobby(
        &self,
        room_id: String,
        addr: SocketAddr,
        name: String,
        is_player: bool,
    ) -> Result<(String, String), String> {
        let mut lobbies = self.lobbies.lock().await;

        let exist = lobbies
            .iter()
            .find(|l| l.players().iter().any(|p| p.addr() == addr.to_string()));

        if exist.is_some() {
            return Err("Player already in a lobby".to_string());
        }

        let lobby = lobbies
            .iter_mut()
            .find(|l| l.room_id() == room_id)
            .ok_or(format!("Lobby {} not found", room_id))?;

        if lobby.is_full() {
            return Err(format!("Lobby {} is full", room_id));
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
        client.room_chat = Some(Room::new(lobby.room_chat_id().clone()));
        client.name = name.clone();

        let room_peer_id = client
            .peer_id(room_id.clone())
            .ok_or("Couldn't retrieve Room peer id")?;
        let room_chat_peer_id = client
            .peer_id(lobby.room_chat_id().clone())
            .ok_or("Couldn't retrieve Room chat peer id")?;

        if is_player {
            lobby.add_player(Player::new(
                client.name.clone(),
                addr.to_string(),
                room_peer_id.clone(),
                room_chat_peer_id.clone(),
                false,
            ));
        } else {
            lobby.add_spectator(Player::new(
                client.name.clone(),
                addr.to_string(),
                room_peer_id.clone(),
                room_chat_peer_id.clone(),
                false,
            ));
        }

        Ok((room_peer_id, room_chat_peer_id))
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

        lobby.remove_player_or_spectator(addr.to_string());

        Ok(lobby.room_id())
    }

    pub async fn initialize_end_game_choice(&self, room_id: String) -> Result<(), String> {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|lobby| lobby.room_id() == room_id)
            .ok_or("Lobby not found".to_string())?;

        if lobby.is_full() && lobby.end_game_choice().is_none() {
            lobby.initialize_end_game_choice();
        }

        Ok(())
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

    pub async fn end_game_action(&self, room_id: String) {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|lobby| lobby.room_id() == room_id)
            .unwrap();

        if let Some(end_game_choice) = lobby.end_game_choice() {
            if end_game_choice.len() < lobby.players().len() {
                return;
            }

            let choices: Vec<EndGameChoice> = end_game_choice
                .iter()
                .map(|(_, choice)| choice.clone())
                .collect();

            if choices.iter().all(|c| *c == EndGameChoice::Rematch) {
                info!(
                    "End game action for room {} : {:?}",
                    room_id,
                    EndGameChoice::Rematch
                );

                self.send_msg_to_lobby(lobby.clone(), ServerMsg::RematchLobby {})
                    .await;
            } else {
                info!(
                    "End game action for room {} : {:?}",
                    room_id,
                    EndGameChoice::Archer
                );

                self.send_msg_to_lobby(lobby.clone(), ServerMsg::ArcherSelectLobby {})
                    .await;
            }

            lobby.reset();
        }
    }

    async fn send_msg_to_lobby(&self, lobby: Lobby, msg: ServerMsg) {
        let mut connexions = self.connections.lock().await;

        for (addr, sender) in connexions.iter_mut() {
            if lobby.players().iter().any(|p| {
                p.addr() == addr.to_string()
                    || lobby
                        .spectators()
                        .iter()
                        .any(|s| s.addr() == addr.to_string())
            }) {
                info!("Sending {:?}  to {}", msg.clone(), addr.to_string());

                sender.send(Message::Item(msg.clone())).await.unwrap();
            }
        }
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

    pub async fn send_lobby_update(&self, room_id: String) -> Result<(), String> {
        let lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter()
            .find(|l| l.room_id() == room_id)
            .ok_or("Lobby not found".to_string())?;

        self.send_msg_to_lobby(
            lobby.clone(),
            ServerMsg::LobbyUpdate {
                lobby: lobby.clone(),
            },
        )
        .await;

        Ok(())
    }

    pub async fn update_player(&self, addr: SocketAddr, player: Player) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|l| l.players().iter().any(|p| p.addr() == addr.to_string()))
            .ok_or("Lobby not found".to_string())?;

        lobby.update_player(player);

        Ok(lobby.room_id())
    }

    pub async fn remove_all_empty_signaling_rooms(&self) {
        let mut signaling = self.signaling.lock().await;

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
