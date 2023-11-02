use async_trait::async_trait;
use std::net::{IpAddr, SocketAddr};

use crate::state::{client::Client, Clients};

#[async_trait]
pub trait ClientsExts {
    async fn get_client(&self, addr: SocketAddr) -> Option<Client>;
    async fn get_client_by_peer_id(&self, peer_id: String) -> Option<Client>;
    async fn remove_client(&mut self, addr: SocketAddr) -> Option<Client>;
    async fn get_client_by_ip_not_live(&self, addr: IpAddr, room_id: String) -> Option<Client>;
    async fn set_client_live_by_ip(&self, addr: IpAddr, room_id: String, is_live: bool);
    async fn set_client_live_by_peer_id(&self, peer_id: String, is_live: bool);
}

#[async_trait]
impl ClientsExts for Clients {
    async fn get_client(&self, addr: SocketAddr) -> Option<Client> {
        let clients = self.lock().await;
        clients.get(&addr).cloned()
    }

    async fn get_client_by_peer_id(&self, peer_id: String) -> Option<Client> {
        let clients = self.lock().await;
        clients
            .iter()
            .find(|(_, client)| client.peer_id(peer_id.clone()).is_some())
            .map(|(_, client)| client.clone())
    }

    async fn get_client_by_ip_not_live(&self, addr: IpAddr, room_id: String) -> Option<Client> {
        let clients = self.lock().await;
        clients
            .iter()
            .filter(|(ip, _client)| ip.ip() == addr)
            .find(|(_, client)| {
                (client.room.is_some()
                    && client.room.as_ref().unwrap().id() == room_id
                    && !client.room.as_ref().unwrap().is_live())
                    || (client.room_chat.is_some()
                        && client.room_chat.as_ref().unwrap().id() == room_id
                        && !client.room_chat.as_ref().unwrap().is_live())
            })
            .map(|(_, client)| client.clone())
    }

    async fn set_client_live_by_ip(&self, addr: IpAddr, room_id: String, is_live: bool) {
        let mut clients = self.lock().await;
        clients
            .iter_mut()
            .filter(|(ip, _client)| ip.ip() == addr)
            .find(|(_, client)| {
                (client.room.is_some()
                    && client.room.as_ref().unwrap().id() == room_id
                    && !client.room.as_ref().unwrap().is_live())
                    || (client.room_chat.is_some()
                        && client.room_chat.as_ref().unwrap().id() == room_id
                        && !client.room_chat.as_ref().unwrap().is_live())
            })
            .map(|(_, client)| client.set_live(room_id, is_live));
    }

    async fn set_client_live_by_peer_id(&self, peer_id: String, is_live: bool) {
        let mut clients = self.lock().await;
        clients
            .iter_mut()
            .find(|(_, client)| client.is_peer_id_live(peer_id.clone()))
            .map(|(_, client)| client.set_live_by_peer_id(peer_id, is_live));
    }

    async fn remove_client(&mut self, addr: SocketAddr) -> Option<Client> {
        let mut clients = self.lock().await;
        clients.remove(&addr)
    }
}
