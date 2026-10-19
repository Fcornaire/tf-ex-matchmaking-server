use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::{Duration, Instant},
};

use tracing::{error, info, warn};

use crate::{
    constants::CONNECTION_CHECK_SECS,
    msg::ServerMsg,
    relay::{Channel, RoomKey},
};

use super::AppState;

const UNREACHABLE_REASON: &str = "Connection failed, check your firewall";

pub type PeerPair = (String, String);

#[derive(Debug, Clone, Copy, PartialEq)]
enum Link {
    Pending(Instant),
    Reached,
    Failed,
}

#[derive(Debug, Default)]
pub struct ConnectivityUpdate {
    pub failed_pairs: Vec<(String, String)>,
    pub unreachable: Vec<String>,
    pub has_relayed_pairs_changed: bool,
    pub has_checking_changed: bool,
    pub is_relay_down: bool,
}

#[derive(Debug, Clone, Default)]
pub struct Connectivity {
    direct: HashMap<PeerPair, Link>,
    relay: HashMap<String, Link>,
    is_relay_down_reported: bool,
}

impl Connectivity {
    pub fn record_reached(&mut self, a: &str, b: &str) {
        let link = self.direct.entry(pair(a, b)).or_insert(Link::Reached);

        if *link != Link::Failed {
            *link = Link::Reached;
        }
    }

    pub fn failed_pairs(&self, peers: &[String]) -> Vec<PeerPair> {
        pairs(peers)
            .into_iter()
            .filter(|pair| self.direct.get(pair) == Some(&Link::Failed))
            .collect()
    }

    pub fn is_checking(&self, peers: &[String], relay_peers: &[String]) -> bool {
        pairs(peers)
            .iter()
            .any(|pair| is_unresolved(self.direct.get(pair)))
            || relay_peers
                .iter()
                .any(|peer| is_unresolved(self.relay.get(peer)))
    }

    pub fn is_settled(&self, peers: &[String], relay_peers: &[String]) -> bool {
        let pairs = pairs(peers);

        self.direct.len() == pairs.len()
            && pairs
                .iter()
                .all(|pair| !is_unresolved(self.direct.get(pair)))
            && self.relay.len() == relay_peers.len()
            && relay_peers
                .iter()
                .all(|peer| !is_unresolved(self.relay.get(peer)))
    }

    pub fn has_working_routes(&self, peers: &[String], relayed: &[PeerPair]) -> bool {
        pairs(peers).iter().all(|pair| {
            self.direct.get(pair) == Some(&Link::Reached)
                || (relayed.contains(pair)
                    && self.relay.get(&pair.0) == Some(&Link::Reached)
                    && self.relay.get(&pair.1) == Some(&Link::Reached))
        })
    }

    pub fn update_direct(&mut self, peers: &[String], now: Instant) -> Vec<PeerPair> {
        let current = pairs(peers);
        self.direct.retain(|pair, _| current.contains(pair));

        let mut failed = vec![];

        for pair in current {
            let link = self
                .direct
                .entry(pair.clone())
                .or_insert(Link::Pending(now));

            if is_expired(*link, now) {
                *link = Link::Failed;
                failed.push(pair);
            }
        }

        failed
    }

    pub fn update_relay(&mut self, peers: &[String], confirmed: &HashSet<String>, now: Instant) {
        self.relay.retain(|peer, _| peers.contains(peer));

        for peer in peers {
            let link = self.relay.entry(peer.clone()).or_insert(Link::Pending(now));

            if confirmed.contains(peer) {
                *link = Link::Reached;
            } else if is_expired(*link, now) {
                *link = Link::Failed;
            }
        }
    }

    pub fn clear_relay(&mut self) {
        self.relay.clear();
        self.is_relay_down_reported = false;
    }

    pub fn relay_unreachable(&self) -> Vec<String> {
        if !self.relay.values().any(|link| *link == Link::Reached) {
            return vec![];
        }

        self.relay
            .iter()
            .filter(|(_, link)| **link == Link::Failed)
            .map(|(peer, _)| peer.clone())
            .collect()
    }

    pub fn should_report_relay_down(&mut self) -> bool {
        let is_down =
            !self.relay.is_empty() && self.relay.values().all(|link| *link == Link::Failed);
        let should_report = is_down && !self.is_relay_down_reported;

        self.is_relay_down_reported = is_down;

        should_report
    }
}

fn pair(a: &str, b: &str) -> PeerPair {
    if a < b {
        (a.to_string(), b.to_string())
    } else {
        (b.to_string(), a.to_string())
    }
}

fn pairs(peers: &[String]) -> Vec<PeerPair> {
    peers
        .iter()
        .enumerate()
        .flat_map(|(index, a)| peers[index + 1..].iter().map(move |b| pair(a, b)))
        .collect()
}

pub fn peers_of(pairs: &[PeerPair]) -> Vec<String> {
    let mut peers: Vec<String> = vec![];

    for peer in pairs.iter().flat_map(|(a, b)| [a, b]) {
        if !peers.contains(peer) {
            peers.push(peer.clone());
        }
    }

    peers
}

fn is_unresolved(link: Option<&Link>) -> bool {
    matches!(link, None | Some(Link::Pending(_)))
}

fn is_expired(link: Link, now: Instant) -> bool {
    matches!(link, Link::Pending(since) if now.duration_since(since) >= Duration::from_secs(CONNECTION_CHECK_SECS))
}

impl AppState {
    pub async fn connectivity_tick(&self) {
        let is_relay_available = self.relay.is_some();

        let mut unreachable: Vec<(String, String)> = vec![];
        let mut checked_series: Vec<String> = vec![];

        {
            let mut lobbies = self.lobbies.lock().await;

            for lobby in lobbies
                .iter_mut()
                .filter(|lobby| !lobby.in_game() && !lobby.is_connectivity_settled())
            {
                let room_id = lobby.room_id();
                let update = lobby
                    .update_connectivity(is_relay_available, &self.relay_confirmed_peers(&room_id));

                for (a, b) in &update.failed_pairs {
                    warn!(
                        "{} and {} could not connect in room {}",
                        self.member_label(a).await,
                        self.member_label(b).await,
                        room_id
                    );
                }

                for addr in &update.unreachable {
                    warn!(
                        "{} cannot reach the relay, removed from room {}",
                        self.member_label(addr).await,
                        room_id
                    );

                    unreachable.push((room_id.clone(), addr.clone()));
                }

                if update.is_relay_down {
                    error!("Nobody in room {room_id} reaches the relay ??");
                }

                if update.has_relayed_pairs_changed {
                    info!(
                        "Room {room_id} relays {} pair(s)",
                        lobby.relayed_pairs().len()
                    );
                }

                let should_broadcast = update.has_relayed_pairs_changed
                    || !update.unreachable.is_empty()
                    || (update.has_checking_changed && lobby.all_players_ready());

                if should_broadcast {
                    self.send_msg_to_lobby(
                        lobby.clone(),
                        ServerMsg::LobbyUpdate {
                            lobby: lobby.clone(),
                        },
                    )
                    .await;
                }

                if update.has_checking_changed
                    && !lobby.is_checking_connections()
                    && lobby.series().is_some()
                {
                    checked_series.push(room_id);
                }
            }
        }

        self.remove_unreachable(unreachable).await;

        for room_id in checked_series {
            self.try_start_series_game(room_id).await;
        }
    }

    pub async fn record_peer_measured(&self, addr: SocketAddr, peer: String) {
        let series_room = {
            let mut lobbies = self.lobbies.lock().await;
            let addr = addr.to_string();

            let Some(lobby) = lobbies
                .iter_mut()
                .find(|lobby| lobby.players().iter().any(|p| p.addr() == addr))
            else {
                return;
            };

            if lobby.in_game() || !lobby.record_measured(&addr, &peer) {
                return;
            }

            if lobby.all_players_ready() {
                self.send_msg_to_lobby(
                    lobby.clone(),
                    ServerMsg::LobbyUpdate {
                        lobby: lobby.clone(),
                    },
                )
                .await;
            }

            lobby.series().is_some().then(|| lobby.room_id())
        };

        if let Some(room_id) = series_room {
            self.try_start_series_game(room_id).await;
        }
    }

    pub async fn is_room_spectator(&self, room_id: &str, peer: &str) -> bool {
        self.lobbies
            .lock()
            .await
            .iter()
            .any(|lobby| lobby.room_id() == room_id && lobby.has_spectator(peer))
    }

    fn relay_confirmed_peers(&self, room_id: &str) -> HashSet<String> {
        let Some(relay) = &self.relay else {
            return HashSet::new();
        };

        [Channel::Match, Channel::PingMeasurement]
            .into_iter()
            .flat_map(|channel| {
                relay.confirmed_peers(&RoomKey {
                    room_id: room_id.to_string(),
                    channel,
                })
            })
            .collect()
    }

    async fn remove_unreachable(&self, unreachable: Vec<(String, String)>) {
        if unreachable.is_empty() {
            return;
        }

        {
            let mut clients = self.clients.lock().await;

            for (room_id, addr) in &unreachable {
                let Ok(addr) = addr.parse::<SocketAddr>() else {
                    continue;
                };

                let client = clients
                    .iter_mut()
                    .find(|((ipv4, ipv6), _)| *ipv4 == addr || *ipv6 == Some(addr))
                    .map(|(_, client)| client);

                if let Some(client) = client {
                    if client
                        .room
                        .as_ref()
                        .is_some_and(|room| room.id() == *room_id)
                    {
                        client.room = None;
                    }
                }
            }
        }

        let addrs: Vec<String> = unreachable.into_iter().map(|(_, addr)| addr).collect();

        self.send_msg_to_addrs(
            &addrs,
            ServerMsg::KickedFromLobby {
                reason: UNREACHABLE_REASON.to_string(),
            },
        )
        .await;

        self.purge_lobby_async().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peers(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    fn after_deadline(now: Instant) -> Instant {
        now + Duration::from_secs(CONNECTION_CHECK_SECS)
    }

    #[test]
    fn checking_until_every_pair_is_measured() {
        let players = peers(&["a", "b", "c"]);
        let mut connectivity = Connectivity::default();

        connectivity.update_direct(&players, Instant::now());
        assert!(connectivity.is_checking(&players, &[]));

        connectivity.record_reached("b", "a");
        connectivity.record_reached("a", "c");
        assert!(connectivity.is_checking(&players, &[]));

        connectivity.record_reached("c", "b");
        assert!(!connectivity.is_checking(&players, &[]));
    }

    #[test]
    fn unmeasured_pair_fails_and_is_forgotten_when_a_player_leaves() {
        let players = peers(&["a", "b", "c"]);
        let now = Instant::now();
        let mut connectivity = Connectivity::default();

        connectivity.update_direct(&players, now);
        connectivity.record_reached("a", "b");
        connectivity.record_reached("a", "c");

        let failed = connectivity.update_direct(&players, after_deadline(now));
        assert_eq!(failed, vec![pair("b", "c")]);
        assert_eq!(connectivity.failed_pairs(&players), vec![pair("b", "c")]);
        assert!(!connectivity.is_checking(&players, &[]));

        connectivity.record_reached("c", "b");
        assert_eq!(connectivity.failed_pairs(&players), vec![pair("b", "c")]);

        connectivity.update_direct(&peers(&["a", "b"]), after_deadline(now));
        assert!(connectivity.failed_pairs(&players).is_empty());
    }

    #[test]
    fn settles_once_nothing_is_left_to_resolve() {
        let players = peers(&["a", "b"]);
        let now = Instant::now();
        let mut connectivity = Connectivity::default();

        connectivity.update_direct(&players, now);
        assert!(!connectivity.is_settled(&players, &[]));

        connectivity.record_reached("a", "b");
        assert!(connectivity.is_settled(&players, &[]));
        assert!(!connectivity.is_settled(&players, &players));

        let confirmed = HashSet::from(["a".to_string(), "b".to_string()]);
        connectivity.update_relay(&players, &confirmed, now);
        assert!(connectivity.is_settled(&players, &players));

        assert!(!connectivity.is_settled(&peers(&["a"]), &peers(&["a"])));
    }

    #[test]
    fn relay_unreachable_players_are_named_unless_nobody_reaches_it() {
        let players = peers(&["a", "b"]);
        let now = Instant::now();
        let mut connectivity = Connectivity::default();

        connectivity.update_relay(&players, &HashSet::new(), now);
        assert!(connectivity.is_checking(&[], &players));

        connectivity.update_relay(&players, &HashSet::new(), after_deadline(now));
        assert!(connectivity.relay_unreachable().is_empty());
        assert!(!connectivity.is_checking(&[], &players));
        assert!(connectivity.should_report_relay_down());
        assert!(!connectivity.should_report_relay_down());

        let confirmed = HashSet::from(["a".to_string()]);
        connectivity.update_relay(&players, &confirmed, after_deadline(now));
        assert_eq!(connectivity.relay_unreachable(), vec!["b".to_string()]);
    }

    #[test]
    fn only_players_of_a_failed_pair_need_the_relay() {
        let players = peers(&["a", "b", "c"]);
        let now = Instant::now();
        let mut connectivity = Connectivity::default();

        connectivity.update_direct(&players, now);
        connectivity.record_reached("a", "b");
        connectivity.record_reached("a", "c");
        connectivity.update_direct(&players, after_deadline(now));

        let failed = connectivity.failed_pairs(&players);
        assert_eq!(failed, vec![pair("b", "c")]);

        let relay_peers = peers_of(&failed);
        assert_eq!(relay_peers, peers(&["b", "c"]));

        let confirmed = HashSet::from(["b".to_string()]);
        connectivity.update_relay(&relay_peers, &confirmed, now);
        assert!(connectivity.is_checking(&players, &relay_peers));

        connectivity.update_relay(&relay_peers, &confirmed, after_deadline(now));
        assert!(!connectivity.is_checking(&players, &relay_peers));
        assert_eq!(connectivity.relay_unreachable(), vec!["c".to_string()]);
        assert!(connectivity.is_settled(&players, &relay_peers));
    }
}
