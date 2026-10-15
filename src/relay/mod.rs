pub mod wire;

use std::{
    collections::HashMap,
    fmt, io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::{Duration, Instant},
};

use rand::Rng;
use serde::Serialize;
use tokio::{net::UdpSocket, sync::mpsc::UnboundedSender};
use tracing::{info, warn};

use self::wire::Upstream;
use crate::constants::MAX_RELAYED_SPECTATORS;

const RECEIVE_BUFFER_SIZE: usize = 65_536;
const MAX_PACKETS_PER_SECOND: u32 = 8_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    Match,
    PingMeasurement,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoomKey {
    pub room_id: String,
    pub channel: Channel,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum ConnectionEvent {
    Welcome { token: u64, udp_port: u16 },
    UdpConfirmed {},
    PeerConnected { peer: String, id: u8 },
    PeerDisconnected { peer: String },
    SpectatorsFull {},
}

#[derive(Debug, PartialEq)]
pub enum JoinError {
    RoomFull,
    SpectatorsFull,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DropReason {
    Malformed,
    UnknownToken,
    RateLimited,
    NoDestination,
    SendFailed,
}

impl DropReason {
    pub const ALL: [DropReason; 5] = [
        DropReason::Malformed,
        DropReason::UnknownToken,
        DropReason::RateLimited,
        DropReason::NoDestination,
        DropReason::SendFailed,
    ];

    pub fn label(self) -> &'static str {
        match self {
            DropReason::Malformed => "malformed",
            DropReason::UnknownToken => "unknown_token",
            DropReason::RateLimited => "rate_limited",
            DropReason::NoDestination => "no_destination",
            DropReason::SendFailed => "send_failed",
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RelayStats {
    pub rooms: usize,
    pub members: usize,
    pub received: u64,
    pub forwarded: u64,
    dropped: [u64; DropReason::ALL.len()],
}

impl RelayStats {
    pub fn dropped(&self, reason: DropReason) -> u64 {
        self.dropped[reason as usize]
    }
}

struct Member {
    peer: String,
    id: u8,
    token: u64,
    is_spectator: bool,
    addr: Option<SocketAddr>,
    events: UnboundedSender<ConnectionEvent>,
    rate_window_start: Instant,
    packets_in_window: u32,
}

impl Member {
    fn is_confirmed(&self) -> bool {
        self.addr.is_some()
    }

    fn notify(&self, event: ConnectionEvent) {
        let _ = self.events.send(event);
    }

    fn is_under_rate_limit(&mut self, now: Instant) -> bool {
        if now.duration_since(self.rate_window_start) >= Duration::from_secs(1) {
            self.rate_window_start = now;
            self.packets_in_window = 0;
        }

        self.packets_in_window += 1;
        self.packets_in_window <= MAX_PACKETS_PER_SECOND
    }
}

#[derive(Default)]
struct Tables {
    rooms: HashMap<RoomKey, Vec<Member>>,
    tokens: HashMap<u64, RoomKey>,
}

#[derive(Default)]
struct Counters {
    received: AtomicU64,
    forwarded: AtomicU64,
    dropped: [AtomicU64; DropReason::ALL.len()],
}

impl Counters {
    fn dropped(&self, reason: DropReason) -> &AtomicU64 {
        &self.dropped[reason as usize]
    }
}

enum Route {
    Forward(SocketAddr),
    Consumed,
    Dropped(DropReason),
}

pub struct Relay {
    sockets: Vec<UdpSocket>,
    udp_port: u16,
    tables: Mutex<Tables>,
    counters: Counters,
}

impl fmt::Debug for Relay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Relay")
            .field("udp_port", &self.udp_port)
            .finish_non_exhaustive()
    }
}

fn receive_loop_count() -> usize {
    if cfg!(unix) {
        std::thread::available_parallelism().map_or(1, |count| count.get())
    } else {
        1
    }
}

fn bind_socket(udp_port: u16) -> io::Result<UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )?;

    #[cfg(unix)]
    socket.set_reuse_port(true)?;

    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::from(([0, 0, 0, 0], udp_port)).into())?;

    UdpSocket::from_std(socket.into())
}

impl Relay {
    pub async fn bind(udp_port: u16) -> io::Result<Arc<Self>> {
        let first = bind_socket(udp_port)?;
        let udp_port = first.local_addr()?.port();
        let mut sockets = vec![first];

        for _ in 1..receive_loop_count() {
            sockets.push(bind_socket(udp_port)?);
        }

        let relay = Arc::new(Self {
            sockets,
            udp_port,
            tables: Mutex::new(Tables::default()),
            counters: Counters::default(),
        });

        for index in 0..relay.sockets.len() {
            tokio::spawn(relay.clone().receive_loop(index));
        }

        info!(
            "Relay: {} receive loop(s) on UDP port {udp_port}",
            relay.sockets.len()
        );

        Ok(relay)
    }

    pub fn udp_port(&self) -> u16 {
        self.udp_port
    }

    pub fn join(
        &self,
        key: RoomKey,
        peer: String,
        is_spectator: bool,
        events: UnboundedSender<ConnectionEvent>,
    ) -> Result<u64, JoinError> {
        let mut tables = self.lock();

        let previous = tables
            .rooms
            .get(&key)
            .and_then(|members| members.iter().find(|member| member.peer == peer))
            .map(|member| member.token);

        if let Some(previous) = previous {
            Self::remove(&mut tables, previous);
        }

        let members = tables.rooms.entry(key.clone()).or_default();

        if is_spectator
            && members.iter().filter(|member| member.is_spectator).count() >= MAX_RELAYED_SPECTATORS
        {
            return Err(JoinError::SpectatorsFull);
        }

        let id = (0..=u8::MAX)
            .find(|id| members.iter().all(|member| member.id != *id))
            .ok_or(JoinError::RoomFull)?;
        let token = rand::thread_rng().gen_range(1..=u64::MAX);

        events
            .send(ConnectionEvent::Welcome {
                token,
                udp_port: self.udp_port,
            })
            .map_err(|_| JoinError::Closed)?;

        members.push(Member {
            peer,
            id,
            token,
            is_spectator,
            addr: None,
            events,
            rate_window_start: Instant::now(),
            packets_in_window: 0,
        });

        tables.tokens.insert(token, key);

        Ok(token)
    }

    pub fn leave(&self, token: u64) {
        Self::remove(&mut self.lock(), token);
    }

    pub fn confirmed_peers(&self, key: &RoomKey) -> Vec<String> {
        self.lock()
            .rooms
            .get(key)
            .map(|members| {
                members
                    .iter()
                    .filter(|member| member.is_confirmed())
                    .map(|member| member.peer.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn stats(&self) -> RelayStats {
        let tables = self.lock();

        RelayStats {
            rooms: tables.rooms.len(),
            members: tables.tokens.len(),
            received: self.counters.received.load(Ordering::Relaxed),
            forwarded: self.counters.forwarded.load(Ordering::Relaxed),
            dropped: DropReason::ALL
                .map(|reason| self.counters.dropped(reason).load(Ordering::Relaxed)),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Tables> {
        self.tables
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn remove(tables: &mut Tables, token: u64) {
        let Some(key) = tables.tokens.remove(&token) else {
            return;
        };

        let Some(members) = tables.rooms.get_mut(&key) else {
            return;
        };

        let Some(index) = members.iter().position(|member| member.token == token) else {
            return;
        };

        let gone = members.swap_remove(index);

        if gone.is_confirmed() {
            for member in members.iter().filter(|member| member.is_confirmed()) {
                member.notify(ConnectionEvent::PeerDisconnected {
                    peer: gone.peer.clone(),
                });
            }
        }

        if members.is_empty() {
            tables.rooms.remove(&key);
        }
    }

    async fn receive_loop(self: Arc<Self>, index: usize) {
        let socket = &self.sockets[index];
        let mut buffer = vec![0u8; RECEIVE_BUFFER_SIZE];
        let mut out = Vec::with_capacity(RECEIVE_BUFFER_SIZE);

        loop {
            let (len, from) = match socket.recv_from(&mut buffer).await {
                Ok(received) => received,
                Err(err) => {
                    warn!("Relay: receive failed: {err}");
                    continue;
                }
            };

            self.counters.received.fetch_add(1, Ordering::Relaxed);

            let counter = match self.route(&buffer[..len], from, &mut out) {
                Route::Forward(to) => match socket.send_to(&out, to).await {
                    Ok(_) => Some(&self.counters.forwarded),
                    Err(_) => Some(self.counters.dropped(DropReason::SendFailed)),
                },
                Route::Consumed => None,
                Route::Dropped(reason) => Some(self.counters.dropped(reason)),
            };

            if let Some(counter) = counter {
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn route(&self, buffer: &[u8], from: SocketAddr, out: &mut Vec<u8>) -> Route {
        let Some(packet) = wire::parse_upstream(buffer) else {
            return Route::Dropped(DropReason::Malformed);
        };

        let token = packet.token();
        let mut tables = self.lock();
        let Tables { rooms, tokens } = &mut *tables;

        let Some(key) = tokens.get(&token) else {
            return Route::Dropped(DropReason::UnknownToken);
        };

        let Some(members) = rooms.get_mut(key) else {
            return Route::Dropped(DropReason::UnknownToken);
        };

        let Some(index) = members.iter().position(|member| member.token == token) else {
            return Route::Dropped(DropReason::UnknownToken);
        };

        if !members[index].is_under_rate_limit(Instant::now()) {
            return Route::Dropped(DropReason::RateLimited);
        }

        if Self::track_address(&mut members[index], from, key) {
            Self::announce(members, index);
        }

        match packet {
            Upstream::Hello { .. } => Route::Consumed,
            Upstream::Data { dst, payload, .. } => {
                let Some(to) = members
                    .iter()
                    .find(|member| member.id == dst && member.token != token)
                    .and_then(|member| member.addr)
                else {
                    return Route::Dropped(DropReason::NoDestination);
                };

                wire::write_downstream(out, members[index].id, payload);

                Route::Forward(to)
            }
        }
    }

    /// Follows the member UDP address
    fn track_address(member: &mut Member, from: SocketAddr, key: &RoomKey) -> bool {
        match member.addr {
            Some(addr) if addr == from => false,
            Some(addr) => {
                info!(
                    "Relay: {} moved from {addr} to {from} in room {}",
                    member.peer, key.room_id
                );
                member.addr = Some(from);
                false
            }
            None => {
                info!(
                    "Relay: {} reachable over UDP in room {}",
                    member.peer, key.room_id
                );
                member.addr = Some(from);
                true
            }
        }
    }

    fn announce(members: &[Member], index: usize) {
        let joined = &members[index];

        joined.notify(ConnectionEvent::UdpConfirmed {});

        for other in members
            .iter()
            .filter(|member| member.token != joined.token && member.is_confirmed())
        {
            joined.notify(ConnectionEvent::PeerConnected {
                peer: other.peer.clone(),
                id: other.id,
            });
            other.notify(ConnectionEvent::PeerConnected {
                peer: joined.peer.clone(),
                id: joined.id,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        sync::mpsc::{unbounded_channel, UnboundedReceiver},
        time::timeout,
    };

    const WAIT: Duration = Duration::from_secs(2);

    struct TestPeer {
        socket: UdpSocket,
        token: u64,
        events: UnboundedReceiver<ConnectionEvent>,
    }

    impl TestPeer {
        async fn join(relay: &Relay, room: &str, peer: &str) -> Self {
            let (tx, mut events) = unbounded_channel();
            let token = relay.join(key(room), peer.to_string(), false, tx).unwrap();

            assert_eq!(
                events.recv().await,
                Some(ConnectionEvent::Welcome {
                    token,
                    udp_port: relay.udp_port()
                })
            );

            Self {
                socket: UdpSocket::bind("127.0.0.1:0").await.unwrap(),
                token,
                events,
            }
        }

        async fn send(&self, relay: &Relay, kind: u8, tail: &[u8]) {
            let mut buffer = vec![kind];
            buffer.extend_from_slice(&self.token.to_le_bytes());
            buffer.extend_from_slice(tail);

            self.socket
                .send_to(&buffer, ("127.0.0.1", relay.udp_port()))
                .await
                .unwrap();
        }

        async fn event(&mut self) -> ConnectionEvent {
            timeout(WAIT, self.events.recv()).await.unwrap().unwrap()
        }

        async fn buffer(&self) -> Vec<u8> {
            let mut buf = [0u8; 256];
            let (len, _) = timeout(WAIT, self.socket.recv_from(&mut buf))
                .await
                .unwrap()
                .unwrap();

            buf[..len].to_vec()
        }
    }

    fn key(room: &str) -> RoomKey {
        RoomKey {
            room_id: room.to_string(),
            channel: Channel::Match,
        }
    }

    #[tokio::test]
    async fn forwards_between_confirmed_members() {
        let relay = Relay::bind(0).await.unwrap();
        let mut peer_a = TestPeer::join(&relay, "room", "a").await;
        let mut peer_b = TestPeer::join(&relay, "room", "b").await;

        peer_a.send(&relay, wire::PACKET_HELLO, &[]).await;
        assert_eq!(peer_a.event().await, ConnectionEvent::UdpConfirmed {});

        peer_b.send(&relay, wire::PACKET_HELLO, &[]).await;
        assert_eq!(peer_b.event().await, ConnectionEvent::UdpConfirmed {});
        assert_eq!(
            peer_b.event().await,
            ConnectionEvent::PeerConnected {
                peer: "a".to_string(),
                id: 0
            }
        );
        assert_eq!(
            peer_a.event().await,
            ConnectionEvent::PeerConnected {
                peer: "b".to_string(),
                id: 1
            }
        );

        peer_a.send(&relay, wire::PACKET_GAME, &[1, 7, 7]).await;
        assert_eq!(peer_b.buffer().await, vec![wire::PACKET_GAME, 0, 7, 7]);

        peer_b.send(&relay, wire::PACKET_GAME, &[0, 8]).await;
        assert_eq!(peer_a.buffer().await, vec![wire::PACKET_GAME, 1, 8]);
    }

    #[tokio::test]
    async fn rooms_and_tokens_are_isolated() {
        let relay = Relay::bind(0).await.unwrap();
        let peer_a = TestPeer::join(&relay, "room", "a").await;
        let peer_b = TestPeer::join(&relay, "other", "b").await;

        peer_a.send(&relay, wire::PACKET_HELLO, &[]).await;
        peer_b.send(&relay, wire::PACKET_HELLO, &[]).await;
        peer_a.send(&relay, wire::PACKET_GAME, &[0, 1]).await;
        peer_a.send(&relay, wire::PACKET_GAME, &[1, 1]).await;

        let peer = TestPeer {
            socket: UdpSocket::bind("127.0.0.1:0").await.unwrap(),
            token: 12345,
            events: unbounded_channel().1,
        };
        peer.send(&relay, wire::PACKET_GAME, &[0, 1]).await;

        tokio::time::sleep(Duration::from_millis(200)).await;

        let stats = relay.stats();
        assert_eq!(stats.rooms, 2);
        assert_eq!(stats.forwarded, 0);
        assert_eq!(stats.dropped(DropReason::NoDestination), 2);
        assert_eq!(stats.dropped(DropReason::UnknownToken), 1);
    }

    #[tokio::test]
    async fn spectator_slots_are_capped_per_room() {
        let relay = Relay::bind(0).await.unwrap();
        let (events, _receiver) = unbounded_channel();
        let join = |room: &str, peer: &str, is_spectator: bool| {
            relay.join(key(room), peer.to_string(), is_spectator, events.clone())
        };

        assert!(join("room", "host", false).is_ok());
        let first = join("room", "first", true).unwrap();
        assert!(join("room", "second", true).is_ok());

        assert_eq!(join("room", "third", true), Err(JoinError::SpectatorsFull));
        assert!(join("room", "player", false).is_ok());
        assert!(join("other", "third", true).is_ok());

        relay.leave(first);
        assert!(join("room", "third", true).is_ok());
    }
}
