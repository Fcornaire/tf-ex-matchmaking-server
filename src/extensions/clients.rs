use async_trait::async_trait;
use rand::Rng;
use std::{collections::HashMap, net::SocketAddr};
use time::{macros::format_description, OffsetDateTime};
use tokio::time::Duration;
use tracing::{debug, error};

use crate::{
    services::geoip::GeoipClient,
    state::{client::Client, Clients},
};

#[async_trait]
pub trait ClientsExts {
    async fn register_direct_async(&mut self, ip: SocketAddr, name: String) -> (String, bool);
    async fn match_with_direct_code_async(
        &mut self,
        ip: SocketAddr,
        code: String,
    ) -> (Option<SocketAddr>, i32, String, String, String);
    async fn register_for_quick_play_async(
        &mut self,
        geoip_client: GeoipClient,
        ip: SocketAddr,
        name: String,
    ) -> bool;
    async fn make_match_for_quick_play_async(&mut self) -> i32;
    async fn get_client(&self, addr: SocketAddr) -> Option<Client>;
    async fn unmatch_quick_play_async(&mut self, addr: SocketAddr);
    async fn remove_client(&mut self, addr: SocketAddr) -> Option<Client>;
    async fn cancel_quick_play_async(&mut self, addr: SocketAddr);
    async fn accept_quick_play_async(
        &mut self,
        addr: SocketAddr,
    ) -> (Option<SocketAddr>, i32, String);
    async fn update_matched_history_async(&mut self, max_duration: Duration) -> i32;
    async fn get_total_matchable_clients_in_quick_play_async(&self) -> i32;
}

#[async_trait]
impl ClientsExts for Clients {
    async fn register_direct_async(&mut self, ip: SocketAddr, name: String) -> (String, bool) {
        let mut clients = self.lock().await;

        if clients.contains_key(&ip) {
            error!("addr {}  already registered", ip);
            return ("".to_string(), false);
        }

        //generate random code for direct connection
        let mut code = rand::thread_rng()
            .sample_iter(&rand::distributions::Uniform::from(b'A'..=b'Z'))
            .take(5)
            .map(char::from)
            .collect::<String>();

        while clients.values().any(|client| client.code == code) {
            code = rand::thread_rng()
                .sample_iter(&rand::distributions::Uniform::from(b'A'..=b'Z'))
                .take(5)
                .map(char::from)
                .collect::<String>();
        }

        clients.insert(ip, Client::new_direct(code.clone(), name));
        (code, true)
    }

    async fn match_with_direct_code_async(
        &mut self,
        ip: SocketAddr,
        code: String,
    ) -> (Option<SocketAddr>, i32, String, String, String) {
        let mut clients = self.lock().await;

        if !clients.contains_key(&ip) {
            error!("addr {}  not registered for direct", ip);
            return (None, -1, "".to_string(), "".to_string(), "".to_string());
        }

        let cloned = clients.clone();

        let opponent = cloned
            .iter()
            .find(|(_, client)| client.code == code && client.is_direct());

        if let Some((opponent_ip, opponent_client)) = opponent {
            let current = clients.get_mut(&ip).unwrap();
            current.set_matched(*opponent_ip);

            if current.seed == -1 && opponent_client.seed == -1 {
                let seed = rand::thread_rng().gen_range(0..100000);
                let id = uuid::Uuid::new_v4().to_string();
                let room_chat_id = uuid::Uuid::new_v4().to_string();

                current.seed = seed;
                current.room_id = Some(id.clone());
                current.room_chat_id = Some(room_chat_id.clone());
            } else {
                if opponent_client.seed != -1 {
                    current.seed = opponent_client.seed;
                    current.room_id = opponent_client.room_id.clone();
                    current.room_chat_id = opponent_client.room_chat_id.clone();
                }
            }

            return (
                Some(*opponent_ip),
                current.seed,
                current.room_id.clone().unwrap(),
                current.room_chat_id.clone().unwrap(),
                opponent_client.name.clone(),
            );
        }

        (None, -1, "".to_string(), "".to_string(), "".to_string())
    }

    async fn register_for_quick_play_async(
        &mut self,
        geoip_client: GeoipClient,
        ip: SocketAddr,
        name: String,
    ) -> bool {
        let mut clients = self.lock().await;

        if clients.contains_key(&ip) {
            error!("addr {}  already registered", ip);
            return false;
        }

        let mut client = Client::new_quickplay(name);

        let geoip = geoip_client.get_location(ip.ip()).await;

        match geoip {
            Ok(location) => {
                client.set_location(location);
                clients.insert(ip, client);
            }
            Err(err) => {
                error!("error when registering for matchmaking: {}", err);
                return false;
            }
        }

        true
    }

    async fn make_match_for_quick_play_async(&mut self) -> i32 {
        let mut clients = self.lock().await;

        let clients_cloned = clients.clone();

        let mut matchs: HashMap<(SocketAddr, SocketAddr), (String, String)> = HashMap::new();
        let mut count = 0;

        clients
            .iter_mut()
            .filter(|(_, client)| {
                client.is_quick_play() && !client.is_matched() && !client.already_have_a_match()
            })
            .for_each(|(ip, client)| {
                if let Some(((match1, match2), (room_id, room_chat_id))) = matchs
                    .iter()
                    .find(|((to_check1, to_check2), _)| (*to_check2) == *ip || (*to_check1) == *ip)
                {
                    if ip == match1 {
                        client.set_match(*match2);
                        client.add_match_history(*match2);
                    } else {
                        client.set_match(*match1);
                        client.add_match_history(*match1);
                    }
                    client.room_id = Some(room_id.to_string());
                    client.room_chat_id = Some(room_chat_id.to_string());

                    return;
                }

                if let Some((matched_ip, _client_matched)) = clients_cloned
                    .iter()
                    .filter(|(ip_candidate, _)| (*ip_candidate) != ip)
                    .filter(|(_, potential_client)| potential_client.is_quick_play())
                    .filter(|(_, potential_client)| !potential_client.already_have_a_match())
                    .filter(|(_, potential_client)| !potential_client.already_matched_with(*ip))
                    // .filter(|(_, to_check)| {
                    //     client.is_near((*to_check).clone(), client.match_max_distance())
                    //         && (*to_check)
                    //             .is_near((client).clone(), (*to_check).match_max_distance())
                    // })
                    .min_by_key(|(_, potential_client)| {
                        potential_client
                            .location
                            .distance_to(client.location.clone()) as i64
                    })
                {
                    client.set_match(*matched_ip);
                    client.add_match_history(*matched_ip);

                    let id = uuid::Uuid::new_v4().to_string();
                    let chat_id = uuid::Uuid::new_v4().to_string();

                    client.room_id = Some(id.clone());
                    client.room_chat_id = Some(chat_id.clone());

                    matchs.insert((*ip, *matched_ip), (id.clone(), chat_id.clone()));
                    count += 1;
                } else {
                    debug!(
                        "no match found for {} with max distance {}",
                        ip,
                        client.match_max_distance()
                    );
                    client.extend_max_match_distance();
                }
            });

        count
    }

    async fn update_matched_history_async(&mut self, max_duration: Duration) -> i32 {
        let mut clients = self.lock().await;

        let duration = time::Duration::new(max_duration.as_secs().try_into().unwrap(), 0);
        let format = format_description!(
            "[year]-[month]-[day] [hour]:[minute]:[second].[subsecond] [offset_hour sign:mandatory]:[offset_minute]:[offset_second]"
        );
        let now = OffsetDateTime::now_utc();
        let mut count = 0;

        clients.iter_mut().for_each(|(_, client)| {
            let matched_history = client.matched_history.clone();

            matched_history.iter().for_each(|(public_ip, time)| {
                let date = OffsetDateTime::parse(time, &format).unwrap();

                match date.checked_add(duration) {
                    Some(date) => {
                        if date <= now {
                            client.remove_match_history(public_ip);
                            count += 1;
                        }
                    }
                    None => {}
                };
            })
        });

        count
    }

    async fn get_client(&self, addr: SocketAddr) -> Option<Client> {
        let clients = self.lock().await;
        clients.get(&addr).cloned()
    }

    async fn remove_client(&mut self, addr: SocketAddr) -> Option<Client> {
        let mut clients = self.lock().await;
        clients.remove(&addr)
    }

    async fn cancel_quick_play_async(&mut self, ip: SocketAddr) {
        let mut clients = self.lock().await;

        if let Some((_, client)) = clients
            .iter_mut()
            .find(|(ip_candidate, _client)| **ip_candidate == ip)
        {
            client.matched.take();
            client.possible_match.take();
        }

        if let Some((_, client)) = clients
            .iter_mut()
            .find(|(_, client)| client.have_a_match_with(ip) || client.is_matched_to(ip))
        {
            client.possible_match.take();
            client.matched.take();
        };
    }

    async fn accept_quick_play_async(
        &mut self,
        ip: SocketAddr,
    ) -> (Option<SocketAddr>, i32, String) {
        let mut clients = self.lock().await;

        if clients.contains_key(&ip) {
            let clone = clients.clone();

            let current = clients.get_mut(&ip).unwrap();

            if let Some(opponent_addr) = current.possible_match.take() {
                let opponent = clone
                    .iter()
                    .find(|(ip_candidate, _client)| **ip_candidate == opponent_addr);

                if let Some((opponent_ip, opponent_client)) = opponent {
                    current.set_matched(*opponent_ip);

                    if current.seed == -1 && opponent_client.seed == -1 {
                        let seed = rand::thread_rng().gen_range(0..100000);
                        current.seed = seed;
                    } else {
                        if opponent_client.seed != -1 {
                            current.seed = opponent_client.seed;
                        }
                    }

                    return (
                        Some(*opponent_ip),
                        current.seed,
                        opponent_client.name.clone(),
                    );
                } else {
                    error!("opponent not found : {}", opponent_addr);
                    return (None, -1, String::from(""));
                }
            } else {
                error!("addr {}  not registered", ip);
                return (None, -1, String::from(""));
            }
        } else {
            error!("No qp match {} found", ip);
            return (None, -1, String::from(""));
        }
    }

    async fn get_total_matchable_clients_in_quick_play_async(&self) -> i32 {
        let clients = self.lock().await;

        clients
            .iter()
            .filter(|(_, client)| {
                client.is_quick_play() && (!client.already_have_a_match() || !client.is_matched())
            })
            .count() as i32
    }

    async fn unmatch_quick_play_async(&mut self, ip: SocketAddr) {
        let mut clients = self.lock().await;

        if let Some(client) = clients.get_mut(&ip) {
            client.unmatch_quick_play();
        }
    }
}
