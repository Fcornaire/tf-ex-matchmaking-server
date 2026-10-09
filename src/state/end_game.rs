use std::{sync::atomic::Ordering, time::Duration};

use metrics::{counter, histogram};
use tracing::info;

use crate::{
    constants::END_GAME_CHOICE_SECONDS, msg::ServerMsg, state::lobby::EndGameChoice, telemetry,
};

use super::AppState;

impl AppState {
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

    pub async fn end_game_action(&self, room_id: String) {
        self.resolve_end_game(room_id, false).await;
    }

    pub async fn start_end_game_countdown(&self, room_id: String) {
        let generation = {
            let mut running = self.end_game_countdowns.lock().await;

            if running.contains_key(&room_id) {
                return;
            }

            let generation = self.end_game_generation.fetch_add(1, Ordering::Relaxed);
            running.insert(room_id.clone(), generation);
            generation
        };

        {
            let mut lobbies = self.lobbies.lock().await;

            if let Some(lobby) = lobbies.iter_mut().find(|l| l.room_id() == room_id) {
                counter!(telemetry::MATCHES_ENDED_TOTAL, "kind" => telemetry::kind_label(lobby.kind()))
                    .increment(1);

                if let Some(secs) = lobby.take_match_duration_secs() {
                    histogram!(telemetry::MATCH_DURATION_SECONDS).record(secs);
                }
            }
        }

        info!(
            "End game choice window open for room {} ({}s)",
            room_id, END_GAME_CHOICE_SECONDS
        );

        let state = self.clone();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(END_GAME_CHOICE_SECONDS)).await;

            let still_current = {
                let running = state.end_game_countdowns.lock().await;
                running.get(&room_id) == Some(&generation)
            };

            if still_current {
                state.resolve_end_game(room_id, true).await;
            }
        });
    }

    pub async fn resolve_end_game(&self, room_id: String, force: bool) {
        let mut running = self.end_game_countdowns.lock().await;

        if force && !running.contains_key(&room_id) {
            return;
        }

        let (remaining, rematch, series_lobby, defaulted) = {
            let mut lobbies = self.lobbies.lock().await;

            let Some(lobby) = lobbies.iter_mut().find(|lobby| lobby.room_id() == room_id) else {
                running.remove(&room_id);
                return;
            };

            if lobby.end_game_choice().is_none() {
                return;
            }

            let votes = lobby.live_end_game_votes();
            let undecided = lobby.players_without_choice();

            if !undecided.is_empty() && !force {
                return;
            }

            let rematch = undecided.is_empty()
                && !votes.is_empty()
                && votes
                    .iter()
                    .all(|vote| vote.choice == EndGameChoice::Rematch);

            let series_lobby = lobby.series().is_some();

            if !lobby.players().is_empty() {
                lobby.reset();

                if series_lobby {
                    if lobby.series().is_some_and(|s| s.awaiting_result) {
                        lobby.abort_series("MISSING RESULT");
                    }

                    lobby.set_in_game(false);
                } else {
                    lobby.set_in_game(rematch);
                }
            }

            (lobby.clone(), rematch, series_lobby, undecided.len())
        };

        running.remove(&room_id);
        drop(running);

        self.clear_match_reports(&room_id).await;

        if remaining.players().is_empty() {
            self.purge_lobby_async().await;

            counter!(telemetry::END_GAME_OUTCOMES_TOTAL, "outcome" => "lobby_closed").increment(1);

            return;
        }

        if defaulted > 0 {
            info!(
                "End game for room {} : {} player(s) -> archer select",
                room_id, defaulted
            );
        }

        let _ = self.send_lobby_update(room_id.clone()).await;

        if series_lobby {
            self.continue_series(remaining).await;
            return;
        }

        if rematch {
            info!(
                "End game action for room {} : {:?}",
                room_id,
                EndGameChoice::Rematch
            );

            counter!(telemetry::END_GAME_OUTCOMES_TOTAL, "outcome" => "rematch").increment(1);
            counter!(telemetry::MATCHES_STARTED_TOTAL, "kind" => telemetry::kind_label(remaining.kind()), "trigger" => "rematch")
                .increment(1);

            self.send_msg_to_lobby(remaining.clone(), ServerMsg::RematchLobby {})
                .await;
        } else {
            info!(
                "End game action for room {} : {:?}",
                room_id,
                EndGameChoice::Archer
            );

            counter!(telemetry::END_GAME_OUTCOMES_TOTAL, "outcome" => "archer_select").increment(1);

            self.send_msg_to_lobby(remaining.clone(), ServerMsg::ArcherSelectLobby {})
                .await;
        }
    }
}
