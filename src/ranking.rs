use std::{any::Any, collections::HashMap, fmt::Debug, net::SocketAddr, sync::Arc};

use async_trait::async_trait;

use crate::{
    msg::{LeaderboardView, PlayerProfile},
    state::{
        client::Client,
        lobby::{Departure, Lobby},
        match_report::FinishedMatch,
        queue::QueueCounts,
        AppState, IpV4V6,
    },
};

pub const RANKED_UNAVAILABLE_MESSAGE: &str = "RANKED IS NOT AVAILABLE ON THIS SERVER";

pub struct DepartureEvent(Box<dyn Any + Send + Sync>);

impl DepartureEvent {
    pub fn new<T: Any + Send + Sync>(event: T) -> Self {
        Self(Box::new(event))
    }

    pub fn take<T: Any>(self) -> Option<T> {
        self.0.downcast::<T>().ok().map(|event| *event)
    }
}

#[async_trait]
pub trait Ranking: Send + Sync + Debug {
    fn describe_metrics(&self) {}

    async fn run_jobs(&self, _state: Arc<AppState>) {
        std::future::pending::<()>().await
    }

    async fn publish_metrics(&self, _state: &AppState) {}

    async fn on_identified(
        &self,
        _state: &AppState,
        _player_id: &str,
        _name: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn enter_ranked(
        &self,
        _state: &AppState,
        _addr: SocketAddr,
        _name: String,
        _is_wide: bool,
    ) -> Result<Option<QueueCounts>, String> {
        Err(RANKED_UNAVAILABLE_MESSAGE.to_string())
    }

    async fn exit_ranked(&self, _state: &AppState, _addr: SocketAddr) {}

    async fn leaderboard(&self, _state: &AppState, _player_id: Option<String>) -> LeaderboardView {
        LeaderboardView::default()
    }

    async fn player_profile(
        &self,
        _state: &AppState,
        _player_id: Option<String>,
        _rank: Option<u32>,
    ) -> Option<PlayerProfile> {
        None
    }

    async fn on_match_reported(&self, _state: &AppState, _report: FinishedMatch) {}

    fn settle_departure(
        &self,
        _lobby: &mut Lobby,
        _member: &str,
        _clients: &HashMap<IpV4V6, Client>,
        _departure: Departure,
    ) -> Option<DepartureEvent> {
        None
    }

    async fn after_departure(
        &self,
        _state: &AppState,
        _addr: SocketAddr,
        _departure: Departure,
        _events: Vec<DepartureEvent>,
    ) {
    }

    async fn note_connection_lost(&self, _state: &AppState, _addr: SocketAddr) {}

    async fn is_lobby_update_held(&self, _state: &AppState, _room_id: &str) -> bool {
        false
    }

    async fn is_series_start_held(&self, _state: &AppState, _room_id: &str) -> bool {
        false
    }

    async fn on_drain_start(&self, _state: &AppState) {}
}

#[derive(Debug)]
pub struct NoRanking;

#[async_trait]
impl Ranking for NoRanking {}
