use std::net::SocketAddr;

use axum_typed_websockets::Message;
use metrics::counter;
use tracing::{info, warn};

use crate::{
    msg::{PlayerPing, ServerMsg, SkinChunk},
    telemetry,
};

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
        let mut connexions = self.connections.lock().await;
        let quiet = matches!(
            msg,
            ServerMsg::PingUpdate { .. } | ServerMsg::QuickPlayStatus { .. }
        );

        for (addr, sender) in connexions.iter_mut() {
            if addrs.iter().any(|kicked| *kicked == addr.to_string()) {
                if !quiet {
                    info!("Sending {:?} to {}", msg.clone(), addr.to_string());
                }

                let _ = sender.send(Message::Item(msg.clone())).await;
            }
        }
    }

    pub(super) async fn send_msg_to_lobby(&self, lobby: Lobby, msg: ServerMsg) {
        let mut connexions = self.connections.lock().await;
        let is_quiet_msg = matches!(
            msg,
            ServerMsg::PingUpdate { .. }
                | ServerMsg::QuickPlayStatus { .. }
                | ServerMsg::SkinChunk { .. }
        );

        for (addr, sender) in connexions.iter_mut() {
            if lobby.players().iter().any(|p| p.addr() == addr.to_string())
                || lobby
                    .spectators()
                    .iter()
                    .any(|s| s.addr() == addr.to_string())
            {
                if !is_quiet_msg {
                    info!("Sending {:?}  to {}", msg.clone(), addr.to_string());
                }

                if let Err(err) = sender.send(Message::Item(msg.clone())).await {
                    warn!("Could not send to {} (maybe disconnected ?): {}", addr, err);
                }
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
            counter!(telemetry::SKIN_CHUNKS_TOTAL, "result" => "rejected").increment(1);
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

        counter!(telemetry::SKIN_CHUNKS_TOTAL, "result" => "relayed").increment(1);

        Ok(())
    }

    pub async fn send_ping_update(&self, room_id: String) -> Result<(), String> {
        let lobbies = self.lobbies.lock().await;

        let lobby = lobbies
            .iter()
            .find(|l| l.room_id() == room_id)
            .ok_or("Lobby not found".to_string())?;

        let pings = lobby
            .players()
            .iter()
            .chain(lobby.spectators().iter())
            .map(|p| PlayerPing {
                addr: p.addr(),
                ping: p.ping(),
            })
            .collect();

        self.send_msg_to_lobby(lobby.clone(), ServerMsg::PingUpdate { pings })
            .await;

        Ok(())
    }
}
