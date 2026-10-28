use std::{
    collections::HashSet,
    net::SocketAddr,
    time::{Duration, Instant},
};

use metrics::counter;
use tracing::{info, warn};

use crate::{
    msg::SeatStats,
    state::{
        lobby::LobbyKind,
        series::{Series, SeriesStatus},
    },
    telemetry,
};

use super::AppState;

const STALE_REPORTS_SECS: u64 = 300;

#[derive(Debug, Clone)]
pub struct MatchReport {
    pub addr: String,
    pub winner_seat: i32,
    pub frame: Option<i32>,
    pub checksum: Option<String>,
    pub scores: Option<Vec<i32>>,
    pub stats: Option<Vec<SeatStats>>,
    pub tower: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct PendingReports {
    pub reports: Vec<MatchReport>,
    pub duration_secs: Option<f64>,
    pub map_id: i32,
    pub mode: i32,
    pub created_at: Instant,
}

/// (seat, addr, name) of a seated player
pub type Seated = (usize, String, String);

/// (seat, player_id, name) of an identified player
pub type Identified = (usize, String, String);

#[derive(Debug, Clone)]
pub enum SeriesReport {
    NotSeries,
    GameRecorded(Series),
    Aborted,
    Finished(Series),
}

#[derive(Debug, Clone)]
pub struct FinishedMatch {
    pub room_id: String,
    pub kind: LobbyKind,
    pub reports: PendingReports,
    pub seated: Vec<Seated>,
    pub winner_seat: i32,
    pub agreed: bool,
    pub result_agreed: bool,
    pub series: SeriesReport,
}

impl AppState {
    pub async fn handle_match_report(
        &self,
        addr: SocketAddr,
        winner_seat: i32,
        frame: Option<i32>,
        checksum: Option<String>,
        scores: Option<Vec<i32>>,
        stats: Option<Vec<SeatStats>>,
        tower: Option<i32>,
    ) {
        let addr_str = addr.to_string();

        let (room_id, kind, seated, devices, duration_secs, map_id, mode) = {
            let lobbies = self.lobbies.lock().await;

            let Some(lobby) = lobbies.iter().find(|l| l.has_member(&addr_str)) else {
                return;
            };

            let devices = lobby.seated_devices();

            if devices < 2 {
                return;
            }

            let seated: Vec<Seated> = lobby
                .players()
                .iter()
                .map(|p| (p.seat(), p.addr(), p.name()))
                .collect();

            (
                lobby.room_id(),
                lobby.kind(),
                seated,
                devices,
                lobby.match_duration_secs(),
                lobby.game_data().map_id,
                lobby.game_data().mode,
            )
        };

        if !seated
            .iter()
            .any(|(_, seat_addr, _)| *seat_addr == addr_str)
        {
            return;
        }

        let completed = {
            let mut pending = self.match_reports.lock().await;

            let entry = pending
                .entry(room_id.clone())
                .or_insert_with(|| PendingReports {
                    reports: vec![],
                    duration_secs,
                    map_id,
                    mode,
                    created_at: Instant::now(),
                });

            if entry.duration_secs.is_none() {
                entry.duration_secs = duration_secs;
            }

            if entry.reports.iter().any(|r| r.addr == addr_str) {
                return;
            }

            entry.reports.push(MatchReport {
                addr: addr_str,
                winner_seat,
                frame,
                checksum,
                scores,
                stats,
                tower,
            });

            if entry.reports.len() >= devices {
                pending.remove(&room_id)
            } else {
                None
            }
        };

        if let Some(pending) = completed {
            self.finalize_match_reports(room_id, kind, pending, seated)
                .await;
        }
    }

    pub async fn clear_match_reports(&self, room_id: &str) {
        if self.match_reports.lock().await.remove(room_id).is_some() {
            counter!(telemetry::MATCH_REPORTS_DROPPED_TOTAL, "reason" => "end_game").increment(1);
        }
    }

    pub async fn purge_stale_match_reports(&self) {
        let live_rooms: HashSet<String> = self
            .lobbies
            .lock()
            .await
            .iter()
            .map(|l| l.room_id())
            .collect();

        let mut pending = self.match_reports.lock().await;

        let mut lobby_gone = 0;
        let mut timed_out = 0;

        pending.retain(|room_id, entry| {
            if !live_rooms.contains(room_id) {
                lobby_gone += 1;
                false
            } else if entry.created_at.elapsed() >= Duration::from_secs(STALE_REPORTS_SECS) {
                timed_out += 1;
                false
            } else {
                true
            }
        });

        if lobby_gone + timed_out > 0 {
            counter!(telemetry::MATCH_REPORTS_DROPPED_TOTAL, "reason" => "lobby_gone")
                .increment(lobby_gone);
            counter!(telemetry::MATCH_REPORTS_DROPPED_TOTAL, "reason" => "timeout")
                .increment(timed_out);
            info!("Purged {} stale match reports", lobby_gone + timed_out);
        }
    }

    async fn finalize_match_reports(
        &self,
        room_id: String,
        kind: LobbyKind,
        pending: PendingReports,
        seated: Vec<Seated>,
    ) {
        let first = &pending.reports[0];
        let others = &pending.reports[1..];

        let same_checksum = others.iter().all(|other| {
            let comparable_frame =
                matches!((first.frame, other.frame), (Some(a), Some(b)) if a == b);

            !comparable_frame
                || match (&first.checksum, &other.checksum) {
                    (Some(a), Some(b)) => a == b,
                    _ => true,
                }
        });

        let same_winner = others
            .iter()
            .all(|other| other.winner_seat == first.winner_seat);
        let same_scores = others
            .iter()
            .all(|other| match (&first.scores, &other.scores) {
                (Some(a), Some(b)) => a == b,
                _ => true,
            });

        let agreed = same_winner && same_checksum;
        let result_agreed = same_winner && same_scores;
        let winner_seat = first.winner_seat;

        if !result_agreed || !same_checksum {
            let mut reports = Vec::with_capacity(pending.reports.len());
            for r in &pending.reports {
                reports.push(format!(
                    "{} says seat {} scores {:?} at frame {:?}",
                    self.member_label(&r.addr).await,
                    r.winner_seat,
                    r.scores,
                    r.frame
                ));
            }

            warn!(
                "Match reports for room {} differ (winner {}, scores {}, checksum {}) : {}",
                room_id,
                same_winner,
                same_scores,
                same_checksum,
                reports.join(", ")
            );
        }

        let series = self
            .record_series_result(
                &room_id,
                result_agreed,
                winner_seat,
                first.scores.clone(),
                pending.duration_secs,
            )
            .await;

        self.ranking
            .on_match_reported(
                self,
                FinishedMatch {
                    room_id,
                    kind,
                    reports: pending,
                    seated,
                    winner_seat,
                    agreed,
                    result_agreed,
                    series,
                },
            )
            .await;
    }

    async fn record_series_result(
        &self,
        room_id: &str,
        agreed: bool,
        winner_seat: i32,
        scores: Option<Vec<i32>>,
        duration_secs: Option<f64>,
    ) -> SeriesReport {
        let mut lobbies = self.lobbies.lock().await;

        let Some(lobby) = lobbies.iter_mut().find(|l| l.room_id() == room_id) else {
            return SeriesReport::NotSeries;
        };

        let map_id = lobby.game_data().map_id;

        let Some(series) = lobby.series_mut() else {
            return SeriesReport::NotSeries;
        };

        if series.status != SeriesStatus::InProgress {
            return SeriesReport::Aborted;
        }

        let abort = |series: &mut Series, reason: &str| {
            warn!("Series in room {} aborted : {}", room_id, reason);
            series.abort(reason);
            counter!(telemetry::SERIES_OUTCOMES_TOTAL, "outcome" => "aborted").increment(1);
            SeriesReport::Aborted
        };

        if !agreed {
            return abort(series, "RESULT MISMATCH");
        }

        if winner_seat < 0 {
            return abort(series, "NO WINNER");
        }

        if let Err(err) = series.record_game(
            map_id,
            winner_seat as usize,
            scores.unwrap_or_default(),
            duration_secs,
        ) {
            warn!("Series game in room {} not recorded : {}", room_id, err);
            return abort(series, "BAD RESULT");
        }

        info!(
            "Series game {} in room {} : tower {} won by seat {}",
            series.games.len(),
            room_id,
            map_id,
            winner_seat
        );

        if series.is_finished() {
            counter!(telemetry::SERIES_OUTCOMES_TOTAL, "outcome" => "finished").increment(1);
            SeriesReport::Finished(series.clone())
        } else {
            SeriesReport::GameRecorded(series.clone())
        }
    }

    pub async fn identified_seats(&self, seated: &[Seated]) -> Vec<Identified> {
        let clients = self.clients.lock().await;
        // Local players share their device's address: only its first player is identified
        let mut identified_addrs = HashSet::new();

        seated
            .iter()
            .filter(|(_, addr, _)| identified_addrs.insert(addr.as_str()))
            .filter_map(|(seat, addr, name)| {
                clients
                    .iter()
                    .find(|((ipv4, ipv6), _)| {
                        ipv4.to_string() == *addr
                            || ipv6.map(|v6| v6.to_string() == *addr).unwrap_or(false)
                    })
                    .and_then(|(_, client)| client.player_id.clone())
                    .map(|id| (*seat, id, name.clone()))
            })
            .collect()
    }
}
