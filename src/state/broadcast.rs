use std::net::SocketAddr;

use axum_typed_websockets::Message;
use tracing::{debug, warn};

use crate::msg::{ServerMsg, SkinChunk};

use super::{lobby::Lobby, AppState};

impl AppState {
    pub async fn notify_host_of_late_spectator(&self, room_id: &str, room_peer_id: &str) {
        let host_addr = {
            let lobbies = self.lobbies.lock().await;
            lobbies
                .iter()
                .find(|l| l.room_id() == room_id && l.in_game())
                .and_then(|l| l.players().iter().find(|p| p.is_host()).map(|p| p.addr()))
        };

        if let Some(addr) = host_addr {
            self.send_msg_to_addrs(
                &[addr],
                ServerMsg::SpectatorJoined {
                    room_peer_id: room_peer_id.to_string(),
                },
            )
            .await;
        }
    }

    pub async fn send_msg_to_addrs(&self, addrs: &[String], msg: ServerMsg) {
        let quiet = matches!(
            msg,
            ServerMsg::PingUpdate { .. } | ServerMsg::QuickPlayStatus { .. }
        );

        let targets: Vec<_> = {
            let connexions = self.connections.lock().await;

            connexions
                .iter()
                .filter(|(addr, _)| addrs.iter().any(|target| *target == addr.to_string()))
                .map(|(addr, sender)| (*addr, sender.clone()))
                .collect()
        };

        for (addr, sender) in targets {
            if !quiet {
                debug!(
                    "Sending {} to {}",
                    msg.kind(),
                    self.client_label(addr).await
                );
            }

            let _ = sender.send(Message::Item(msg.clone())).await;
        }
    }

    pub(super) async fn send_msg_to_lobby(&self, lobby: Lobby, msg: ServerMsg) {
        let is_quiet_msg = matches!(
            msg,
            ServerMsg::PingUpdate { .. }
                | ServerMsg::QuickPlayStatus { .. }
                | ServerMsg::SkinChunk { .. }
        );

        let targets: Vec<_> = {
            let connexions = self.connections.lock().await;

            connexions
                .iter()
                .filter(|(addr, _)| lobby.has_member(&addr.to_string()))
                .map(|(addr, sender)| (*addr, sender.clone()))
                .collect()
        };

        if !is_quiet_msg {
            debug!(
                "Sending {} to room {} ({} members)",
                msg.kind(),
                lobby.room_id(),
                targets.len()
            );
        }

        for (addr, sender) in targets {
            if let Err(err) = sender.send(Message::Item(msg.clone())).await {
                warn!(
                    "Could not send to {} (maybe disconnected ?): {}",
                    self.client_label(addr).await,
                    err
                );
            }
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

    pub async fn relay_skin_chunk(&self, addr: SocketAddr, chunk: SkinChunk) -> Result<(), String> {
        const MAX_SKIN_CHUNK_DATA: usize = 400_000;
        const MAX_SKIN_CHUNKS: u32 = 32;

        if chunk.chunk_count == 0
            || chunk.chunk_count > MAX_SKIN_CHUNKS
            || chunk.chunk_index >= chunk.chunk_count
            || chunk.data.len() > MAX_SKIN_CHUNK_DATA
            || chunk.bundle_id.len() > 128
            || chunk.custom_archer_id.len() > 256
        {
            return Err("Skin chunk rejected by size limits".to_string());
        }

        let lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter()
            .find(|l| l.players().iter().any(|p| p.addr() == addr.to_string()))
            .ok_or("Lobby not found".to_string())?;

        let from = lobby
            .players()
            .iter()
            .find(|p| p.addr() == addr.to_string())
            .map(|p| p.room_peer_id())
            .ok_or("Sender not found".to_string())?;

        let lobby = lobby.clone();
        drop(lobbies);

        self.send_msg_to_lobby(lobby, ServerMsg::SkinChunk { chunk, from })
            .await;

        Ok(())
    }

    pub async fn send_ping_update(&self, room_id: String) -> Result<(), String> {
        let lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter()
            .find(|l| l.room_id() == room_id)
            .ok_or("Lobby not found".to_string())?;

        let pings = lobby.member_pings();

        self.send_msg_to_lobby(lobby.clone(), ServerMsg::PingUpdate { pings })
            .await;

        Ok(())
    }
}
