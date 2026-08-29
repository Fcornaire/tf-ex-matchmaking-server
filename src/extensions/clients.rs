use async_trait::async_trait;
use std::net::SocketAddr;

use crate::state::{client::Client, Clients};

#[async_trait]
pub trait ClientsExts {
    async fn get_client(&self, addr: SocketAddr) -> Option<Client>;
    async fn get_client_in_room(&self, room_id: String) -> Option<Client>;
    async fn get_client_by_room_peer(&self, room_id: String, peer_id: String) -> Option<Client>;
    async fn remove_client(&mut self, addr: SocketAddr) -> Option<Client>;
    async fn set_client_live_by_peer_id(&self, peer_id: String, is_live: bool);
}

#[async_trait]
impl ClientsExts for Clients {
    async fn get_client(&self, addr: SocketAddr) -> Option<Client> {
        let clients = self.lock().await;
        clients
            .iter()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
            .map(|(_, client)| client.clone())
    }

    async fn get_client_in_room(&self, room_id: String) -> Option<Client> {
        let clients = self.lock().await;
        clients
            .iter()
            .find(|(_, client)| client.peer_id(room_id.clone()).is_some())
            .map(|(_, client)| client.clone())
    }

    async fn get_client_by_room_peer(&self, room_id: String, peer_id: String) -> Option<Client> {
        let clients = self.lock().await;
        clients
            .iter()
            .find(|(_, client)| {
                client.peer_id(room_id.clone()).as_deref() == Some(peer_id.as_str())
            })
            .map(|(_, client)| client.clone())
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

        let to_remove_key = clients
            .iter()
            .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
            .map(|(key, _)| key.clone())?;

        clients.remove(&to_remove_key)
    }
}
