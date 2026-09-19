use std::{collections::HashMap, net::SocketAddr, time::Instant};

use metrics::{counter, histogram};
use rand::Rng;
use tracing::info;

use crate::{
    constants::QUICK_PLAY_AUTO_START_GRACE_SECS,
    msg::ServerMsg,
    services::geoip::Location,
    state::lobby::{GameData, LobbyBuilder, LobbyKind},
    telemetry,
};

use super::{
    client::Room,
    lobby::{self, Lobby, Player},
    AppState, QueuedPlayer,
};

pub const WIDER_SET_MOD_NAME: &str = "Teuria.WiderSet";

/// WiderSet changes level geometry, so wide and standard players can never share a match
fn lobby_is_wide(lobby: &Lobby) -> bool {
    lobby.mods().iter().any(|m| {
        m.name() == WIDER_SET_MOD_NAME
            && m.data()
                .get("IsWide")
                .map(|value| value.eq_ignore_ascii_case("true"))
                .unwrap_or(false)
    })
}

#[derive(Debug)]
pub enum QuickPlayOutcome {
    Queued {
        searching: u32,
    },
    Seated {
        lobby: Lobby,
        room_peer_id: String,
    },
    Paired {
        lobby: Lobby,
        caller_peer_id: String,
        partner_addr: String,
        partner_peer_id: String,
    },
    Failed {
        message: String,
    },
}

pub fn should_auto_start(
    player_count: usize,
    is_full: bool,
    secs_since_all_ready: u64,
    secs_since_membership_change: u64,
    grace_secs: u64,
) -> bool {
    player_count >= 2
        && (is_full
            || (secs_since_all_ready >= grace_secs && secs_since_membership_change >= grace_secs))
}

impl AppState {
    pub async fn enter_quick_play(
        &self,
        addr: SocketAddr,
        name: String,
        is_wide: bool,
    ) -> QuickPlayOutcome {
        let location = {
            let clients = self.clients.lock().await;

            match clients
                .iter()
                .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
            {
                Some((_, client)) => client.location.clone(),
                None => {
                    return QuickPlayOutcome::Failed {
                        message: "Client not found".to_string(),
                    }
                }
            }
        };

        let mut queue = self.queue.lock().await;
        let mut lobbies = self.lobbies.lock().await;

        if lobbies.iter().any(|l| l.has_member(&addr.to_string())) {
            return QuickPlayOutcome::Failed {
                message: "Already in a lobby".to_string(),
            };
        }

        queue.retain(|q| q.addr != addr);

        if let Some(lobby) = Self::nearest_open_quick_play(&mut lobbies, &location, is_wide) {
            return match self.seat_member(lobby, addr, name, true).await {
                Ok(room_peer_id) => QuickPlayOutcome::Seated {
                    lobby: lobby.clone(),
                    room_peer_id,
                },
                Err(message) => QuickPlayOutcome::Failed { message },
            };
        }

        let partner_index = queue
            .iter()
            .enumerate()
            .filter(|(_, q)| q.is_wide == is_wide)
            .min_by_key(|(_, q)| q.location.distance_to(location.clone()) as i64)
            .map(|(index, _)| index);

        if let Some(partner_index) = partner_index {
            let partner = queue.remove(partner_index);

            let mut clients = self.clients.lock().await;

            let room_id = uuid::Uuid::new_v4().to_string();

            let partner_client = clients
                .iter_mut()
                .find(|((ipv4, ipv6), _)| {
                    *ipv4 == partner.addr || ipv6.is_some() && ipv6.unwrap() == partner.addr
                })
                .map(|(_, client)| client);

            let Some(partner_client) = partner_client else {
                drop(clients);
                queue.push(QueuedPlayer {
                    addr,
                    name,
                    location,
                    is_wide,
                    queued_at: Instant::now(),
                });

                return QuickPlayOutcome::Queued {
                    searching: queue.len() as u32,
                };
            };

            partner_client.room = Some(Room::new(room_id.clone()));
            partner_client.name = partner.name.clone();
            let partner_peer_id = partner_client.peer_id(room_id.clone()).unwrap();

            let caller_client = clients
                .iter_mut()
                .find(|((ipv4, ipv6), _)| *ipv4 == addr || ipv6.is_some() && ipv6.unwrap() == addr)
                .map(|(_, client)| client);

            let Some(caller_client) = caller_client else {
                return QuickPlayOutcome::Failed {
                    message: "Client not found".to_string(),
                };
            };

            caller_client.room = Some(Room::new(room_id.clone()));
            caller_client.name = name.clone();
            let caller_peer_id = caller_client.peer_id(room_id.clone()).unwrap();

            drop(clients);

            let mut host = Player::new(
                partner.name.clone(),
                partner.addr.to_string(),
                partner_peer_id.clone(),
                true,
            );
            host.set_seat(0);

            let mods = if is_wide {
                vec![lobby::Mod::new(
                    WIDER_SET_MOD_NAME.to_string(),
                    HashMap::from([("IsWide".to_string(), "True".to_string())]),
                )]
            } else {
                vec![]
            };

            let mut lobby = LobbyBuilder::new()
                .name("QUICK PLAY".to_string())
                .room_id(room_id)
                .game_data(GameData {
                    map_id: -1,
                    mode: 3,
                    match_length: 2,
                    variants: vec![],
                    seed: rand::thread_rng().gen_range(0..100000),
                    best_of: 0,
                })
                .players(vec![host])
                .kind(LobbyKind::QuickPlay)
                .origin(Some(partner.location.clone()))
                .mods(mods)
                .build();

            lobby.add_player(Player::new(
                name.clone(),
                addr.to_string(),
                caller_peer_id.clone(),
                false,
            ));

            lobbies.push(lobby.clone());

            histogram!(telemetry::QUICKPLAY_WAIT_SECONDS)
                .record(partner.queued_at.elapsed().as_secs_f64());

            return QuickPlayOutcome::Paired {
                lobby,
                caller_peer_id,
                partner_addr: partner.addr.to_string(),
                partner_peer_id,
            };
        }

        queue.push(QueuedPlayer {
            addr,
            name,
            location,
            is_wide,
            queued_at: Instant::now(),
        });

        QuickPlayOutcome::Queued {
            searching: queue.len() as u32,
        }
    }

    pub async fn exit_quick_play(&self, addr: SocketAddr) -> u32 {
        let mut queue = self.queue.lock().await;

        queue.retain(|q| q.addr != addr);

        queue.len() as u32
    }

    fn nearest_open_quick_play<'a>(
        lobbies: &'a mut Vec<Lobby>,
        location: &Location,
        is_wide: bool,
    ) -> Option<&'a mut Lobby> {
        lobbies
            .iter_mut()
            .filter(|l| {
                l.kind() == LobbyKind::QuickPlay
                    && !l.in_game()
                    && !l.is_full()
                    && lobby_is_wide(l) == is_wide
            })
            .min_by_key(|l| {
                l.origin()
                    .map(|origin| origin.distance_to(location.clone()) as i64)
                    .unwrap_or(i64::MAX)
            })
    }

    pub async fn quick_play_tick(&self) {
        self.drain_queue_into_open_lobbies().await;
        self.auto_start_quick_play_lobbies().await;
        self.push_queue_status().await;
    }

    async fn drain_queue_into_open_lobbies(&self) {
        let mut seated: Vec<(String, Lobby, String)> = vec![];

        {
            let mut queue = self.queue.lock().await;
            let mut lobbies = self.lobbies.lock().await;

            let mut remaining: Vec<QueuedPlayer> = vec![];
            let waiting: Vec<QueuedPlayer> = queue.drain(..).collect();

            for queued in waiting {
                let target =
                    Self::nearest_open_quick_play(&mut lobbies, &queued.location, queued.is_wide);

                match target {
                    Some(lobby) => {
                        match self
                            .seat_member(lobby, queued.addr, queued.name.clone(), true)
                            .await
                        {
                            Ok(peer) => {
                                histogram!(telemetry::QUICKPLAY_WAIT_SECONDS)
                                    .record(queued.queued_at.elapsed().as_secs_f64());

                                seated.push((queued.addr.to_string(), lobby.clone(), peer))
                            }
                            Err(_) => remaining.push(queued),
                        }
                    }
                    None => remaining.push(queued),
                }
            }

            *queue = remaining;
        }

        for (addr, lobby, peer) in seated {
            info!("Quickplay drain seated {} into {}", addr, lobby.room_id());

            self.send_msg_to_addrs(
                &[addr],
                ServerMsg::QuickPlayMatchFound {
                    lobby: lobby.clone(),
                    room_peer_id: peer,
                },
            )
            .await;

            let _ = self.send_lobby_update(lobby.room_id()).await;
        }
    }

    async fn auto_start_quick_play_lobbies(&self) {
        let mut started: Vec<Lobby> = vec![];

        {
            let mut lobbies = self.lobbies.lock().await;

            for lobby in lobbies.iter_mut() {
                if lobby.kind() != LobbyKind::QuickPlay || lobby.in_game() {
                    continue;
                }

                let Some(secs_since_all_ready) = lobby.secs_since_all_ready() else {
                    continue;
                };

                if !should_auto_start(
                    lobby.players().len(),
                    lobby.is_full(),
                    secs_since_all_ready,
                    lobby.secs_since_membership_change(),
                    QUICK_PLAY_AUTO_START_GRACE_SECS,
                ) {
                    continue;
                }

                if lobby.end_game_choice().is_none() {
                    lobby.initialize_end_game_choice();
                }

                lobby.set_in_game(true);
                started.push(lobby.clone());
            }
        }

        for lobby in started {
            info!("Auto starting quickplay lobby {}", lobby.room_id());

            counter!(telemetry::MATCHES_STARTED_TOTAL, "kind" => telemetry::kind_label(lobby.kind()), "trigger" => "auto")
                .increment(1);

            self.send_msg_to_lobby(lobby, ServerMsg::StartLobby {})
                .await;
        }
    }

    async fn push_queue_status(&self) {
        let (addrs, searching) = {
            let queue = self.queue.lock().await;

            (
                queue.iter().map(|q| q.addr.to_string()).collect::<Vec<_>>(),
                queue.len() as u32,
            )
        };

        if addrs.is_empty() {
            return;
        }

        self.send_msg_to_addrs(
            &addrs,
            ServerMsg::QuickPlayStatus {
                queued: true,
                searching,
                message: "".to_string(),
            },
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::test_support::*;

    #[test]
    fn auto_start_needs_two_players_and_full_or_both_graces() {
        assert!(!should_auto_start(1, false, 100, 100, 5));
        assert!(!should_auto_start(2, false, 2, 100, 5));
        assert!(!should_auto_start(2, false, 100, 2, 5));
        assert!(should_auto_start(2, false, 5, 5, 5));
        assert!(should_auto_start(4, true, 0, 0, 5));
    }

    #[tokio::test]
    async fn quick_play_pairs_then_fills_and_hides_from_browser() {
        let first = test_addr(1);
        let second = test_addr(2);
        let third = test_addr(3);
        let state = state_with_clients(&[first, second, third]).await;

        let queued = state
            .enter_quick_play(first, "first".to_string(), false)
            .await;
        assert!(matches!(queued, QuickPlayOutcome::Queued { searching: 1 }));

        let paired = state
            .enter_quick_play(second, "second".to_string(), false)
            .await;
        let QuickPlayOutcome::Paired {
            lobby,
            caller_peer_id,
            partner_addr,
            partner_peer_id,
        } = paired
        else {
            panic!("expected a pair");
        };

        assert_eq!(partner_addr, first.to_string());
        assert_ne!(caller_peer_id, partner_peer_id);
        assert_eq!(lobby.players().len(), 2);
        assert_eq!(lobby.kind(), LobbyKind::QuickPlay);
        assert!(lobby
            .players()
            .iter()
            .any(|p| p.is_host() && p.addr() == first.to_string()));
        assert!(state.get_lobbies(first).await.is_empty());

        let seated = state
            .enter_quick_play(third, "third".to_string(), false)
            .await;
        let QuickPlayOutcome::Seated { lobby, .. } = seated else {
            panic!("expected a seat in the open lobby");
        };

        assert_eq!(lobby.players().len(), 3);
    }

    #[tokio::test]
    async fn quick_play_host_migrates_on_leave() {
        let first = test_addr(1);
        let second = test_addr(2);
        let state = state_with_clients(&[first, second]).await;

        state
            .enter_quick_play(first, "first".to_string(), false)
            .await;
        state
            .enter_quick_play(second, "second".to_string(), false)
            .await;

        state.remove_from_lobby(first).await.unwrap();

        let lobbies = state.lobbies.lock().await;
        let lobby = lobbies.first().unwrap();

        assert_eq!(lobby.players().len(), 1);
        assert!(lobby
            .players()
            .iter()
            .any(|p| p.is_host() && p.addr() == second.to_string()));
    }

    #[tokio::test]
    async fn quick_play_guards_double_entry_and_exit() {
        let first = test_addr(1);
        let second = test_addr(2);
        let state = state_with_clients(&[first, second]).await;

        state
            .enter_quick_play(first, "first".to_string(), false)
            .await;
        let requeued = state
            .enter_quick_play(first, "first".to_string(), false)
            .await;
        assert!(matches!(
            requeued,
            QuickPlayOutcome::Queued { searching: 1 }
        ));

        assert_eq!(state.exit_quick_play(first).await, 0);

        state
            .enter_quick_play(first, "first".to_string(), false)
            .await;
        state
            .enter_quick_play(second, "second".to_string(), false)
            .await;

        let in_lobby = state
            .enter_quick_play(first, "first".to_string(), false)
            .await;
        assert!(matches!(in_lobby, QuickPlayOutcome::Failed { .. }));
    }

    #[tokio::test]
    async fn quick_play_never_mixes_wide_and_standard() {
        let first = test_addr(1);
        let second = test_addr(2);
        let third = test_addr(3);
        let state = state_with_clients(&[first, second, third]).await;

        let queued = state
            .enter_quick_play(first, "first".to_string(), true)
            .await;
        assert!(matches!(queued, QuickPlayOutcome::Queued { searching: 1 }));

        let still_queued = state
            .enter_quick_play(second, "second".to_string(), false)
            .await;
        assert!(matches!(
            still_queued,
            QuickPlayOutcome::Queued { searching: 2 }
        ));

        let paired = state
            .enter_quick_play(third, "third".to_string(), true)
            .await;
        let QuickPlayOutcome::Paired {
            lobby,
            partner_addr,
            ..
        } = paired
        else {
            panic!("expected the two wide players to pair");
        };

        assert_eq!(partner_addr, first.to_string());
        assert!(lobby_is_wide(&lobby));
        assert_eq!(state.exit_quick_play(second).await, 0);
    }

    #[tokio::test]
    async fn quick_play_auto_starts_after_grace() {
        let first = test_addr(1);
        let second = test_addr(2);
        let state = state_with_clients(&[first, second]).await;

        state
            .enter_quick_play(first, "first".to_string(), false)
            .await;
        state
            .enter_quick_play(second, "second".to_string(), false)
            .await;

        state.quick_play_tick().await;
        assert!(!state.lobbies.lock().await.first().unwrap().in_game());

        {
            let mut lobbies = state.lobbies.lock().await;
            let lobby = lobbies.first_mut().unwrap();
            lobby.set_all_ready();
            lobby.backdate_membership(QUICK_PLAY_AUTO_START_GRACE_SECS);
        }

        state.quick_play_tick().await;
        assert!(!state.lobbies.lock().await.first().unwrap().in_game());

        {
            let mut lobbies = state.lobbies.lock().await;
            let lobby = lobbies.first_mut().unwrap();
            lobby.backdate_ready(QUICK_PLAY_AUTO_START_GRACE_SECS);
        }

        state.quick_play_tick().await;

        let lobbies = state.lobbies.lock().await;
        let lobby = lobbies.first().unwrap();

        assert!(lobby.in_game());
        assert!(lobby.end_game_choice().is_some());
    }
}
