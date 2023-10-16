pub mod client;
pub mod lobby;
pub mod signaling_state;

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use axum_typed_websockets::Message;
use rand::Rng;
use tokio::sync::{mpsc::Sender, Mutex};
use tracing::info;

use crate::{msg::ServerMsg, services::geoip::GeoipClient, state::lobby::LobbyBuilder};

use self::{
    client::{Client, Room},
    lobby::{Lobby, Player},
    signaling_state::SignalingState,
};

pub type Clients = Arc<Mutex<HashMap<SocketAddr, Client>>>;
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

    pub async fn update_alive_client(&self, addr: SocketAddr) -> Result<(), String> {
        let mut clients = self.clients.lock().await;

        let client_res = clients.get_mut(&addr);

        if client_res.is_none() {
            return Err("Client not found".to_string());
        }

        let client = client_res.unwrap();

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

    pub async fn add_new_client(&self, addr: SocketAddr) -> Result<(), String> {
        let location = self.geoip_client.get_location(addr.ip()).await;

        if location.is_err() {
            return Err("Could not get location".to_string());
        }

        let client = Client::new(location.unwrap());

        self.clients.lock().await.insert(addr, client);

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
        let client = clients.get_mut(&addr).unwrap();

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
            .ok_or("Lobby not found".to_string())?;

        let mut clients: tokio::sync::MutexGuard<'_, HashMap<SocketAddr, Client>> =
            self.clients.lock().await;

        let client_res = clients.get_mut(&addr);

        if client_res.is_none() {
            return Err("Client not found".to_string());
        }

        let client = client_res.unwrap();

        client.room = Some(Room::new(room_id.clone()));
        client.room_chat = Some(Room::new(lobby.room_chat_id().clone()));
        client.name = name.clone();

        let room_peer_id = client
            .peer_id(room_id.clone())
            .ok_or("Couldn't retrieve Room peer id")?;
        let room_chat_peer_id = client
            .peer_id(lobby.room_chat_id().clone())
            .ok_or("Couldn't retrieve Room chat peer id")?;

        lobby.add_player(Player::new(
            client.name.clone(),
            addr.to_string(),
            room_peer_id.clone(),
            room_chat_peer_id.clone(),
            false,
        ));

        Ok((room_peer_id, room_chat_peer_id))
    }

    pub async fn remove_from_lobby(&self, addr: SocketAddr) -> Result<String, String> {
        let mut lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter_mut()
            .find(|l| l.players().iter().any(|p| p.addr() == addr.to_string()))
            .ok_or("Lobby not found".to_string())?;

        let mut clients = self.clients.lock().await;

        let client_res = clients.get_mut(&addr);

        if client_res.is_none() {
            return Err("Client not found".to_string());
        }

        let client = client_res.unwrap();

        client.room = None;

        lobby.remove_player(addr.to_string());

        Ok(lobby.room_id())
    }

    pub async fn purge_lobby(&self) {
        let mut lobbies = self.lobbies.lock().await;

        let count = lobbies.len();
        lobbies.retain(|l| !l.players().is_empty());
        info!("Purged {} empty lobbies", count - lobbies.len());

        let count = lobbies.len();
        lobbies.retain(|l| l.players().iter().any(|p| p.is_host()));
        info!("Purged {} lobbies with no host", count - lobbies.len());
    }

    pub async fn send_lobby_update(&self, room_id: String) -> Result<(), String> {
        let lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter()
            .find(|l| l.room_id() == room_id)
            .ok_or("Lobby not found".to_string())?;

        let mut connexions = self.connections.lock().await;

        for (addr, sender) in connexions.iter_mut() {
            if lobby.players().iter().any(|p| p.addr() == addr.to_string()) {
                info!("Sending lobby update to {}", addr.to_string());

                sender
                    .send(Message::Item(ServerMsg::LobbyUpdate {
                        lobby: lobby.clone(),
                    }))
                    .await
                    .unwrap();
            }
        }

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
