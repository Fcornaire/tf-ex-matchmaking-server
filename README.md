<!-- Shield -->

[![Support me on Patreon](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Fshieldsio-patreon.vercel.app%2Fapi%3Fusername%3DDShadModdingAdventure%26type%3Dpatrons&style=for-the-badge)](https://patreon.com/DShadModdingAdventure)
[![Contributors][contributors-shield]][contributors-url]
[![Download][download-shield]][download-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![GPL-3.0 License][license-shield]][license-url]

# tf-ex-matchmaking-server

Matchmaking and WebRTC signaling server for **TF.EX**, the TowerFall Ascension netplay mod.

The server only handles matchmaking and connection setup, actual gameplay traffic is peer-to-peer between players (WebRTC data channels, STUN-only NAT traversal).

## What it does

- **Lobby browser** : public lobbies players can create, join, and spectate
- **Private lobbies** : joinable by 5-character code
- **Quickplay** : a queue that matches players automatically, ranking candidates by geographic proximity when GeoIP is configured
- **Keep-alive RTT tracking** : feeds the in-game automatic input delay suggestion
- **Prometheus metrics** : optional, served on a separate port

## Quick start

No configuration is required, the server runs out of the box:

Either use it from the release

Or

```sh
cargo run --release
```

Or with Docker:

```sh
docker build -t tf-ex-matchmaking-server .
docker run -p 3000:3000 tf-ex-matchmaking-server
```

## Configuration

Everything is configured through environment variables. All of them are optional:

| Variable                | Default                | Purpose                                                                    |
| ----------------------- | ---------------------- | -------------------------------------------------------------------------- |
| `PORT`                  | `3000`                 | Game facing port (matchmaking + signaling websockets)                      |
| `METRICS_PORT`          | `4738`                 | Prometheus metrics port (keep it private)                                  |
| `GEOIP_CLIENT_ID`       | _(unset)_              | MaxMind account ID, enables GeoIP                                          |
| `GEOIP_API_KEY`         | _(unset)_              | MaxMind license key, enables GeoIP                                         |
| `RELAY_PORT`            | `4537`                 | UDP port of the relay                                                      |
| `REPORTS_DIR`           | _(unset)_              | Folder for crash/desync reports, unset to disable                          |
| `REPORTS_MAX_MB`        | `750`                  | Size cap of the reports folder                                             |
| `MIN_CLIENT_VERSION`    | `0.0.0`                | Oldest TF.EX version allowed to connect                                    |
| `RELAY_PUBLIC_PORTS`    | _(unset)_              | UDP port(s) players reach the relay on, when it sits behind a port mapping |
| `RELAY_PORT_CHECK_HOST` | `host.docker.internal` | Host used to find the public relay port within `RELAY_PUBLIC_PORTS`        |
| `DRAIN_DEADLINE_SECS`   | `2640`                 | On SIGTERM, how long running games may finish before the server exits      |

## Relay (optional)

Players normally connect to each other directly (WebRTC). Some pairs can't, behind strict NATs (or weird network stuff). A lobby on the relay sends its game traffic through the server instead

Before any match, every pair of players must measure its ping

Spectators try both paths and keep the relay only when they can't reach the host directly

## GeoIP (optional but recommended if you want quickplay by region)

Quickplay ranks candidate opponents by geographic distance. That distance comes from a GeoIP lookup of each connecting player's IP.

**Without GeoIP** the server still runs fine and quickplay still works but it just treats every player as same distance, so matches are made with no regard for region or ping.

**To enable it**, create a free [MaxMind GeoLite2 account](https://www.maxmind.com/en/geolite2/signup), generate a license key, and set:

```sh
GEOIP_CLIENT_ID=<your MaxMind account ID>
GEOIP_API_KEY=<your MaxMind license key>
```

## Metrics (optional)

A Prometheus endpoint is served on `METRICS_PORT` (default `4738`) at `/metrics`.

Scrape config:

```yaml
scrape_configs:
  - job_name: tf-matchmaking
    static_configs:
      - targets: ["127.0.0.1:4738"]
```

## Endpoints

| Endpoint                               | Port           | Purpose                                                                |
| -------------------------------------- | -------------- | ---------------------------------------------------------------------- |
| `GET /ws`                              | `PORT`         | Matchmaking websocket (lobby browser, quickplay, chat with the server) |
| `GET /room/:room_id`                   | `PORT`         | WebRTC signaling room for one lobby                                    |
| `GET /ping_measurement/:room_id`       | `PORT`         | Lobby ping measurement between players                                 |
| `GET /relay/room/:room_id`             | `PORT`         | Relay connection for one lobby (data over UDP `RELAY_PORT`)            |
| `GET /relay/ping_measurement/:room_id` | `PORT`         | Lobby ping measurement through the relay                               |
| `POST /report`                         | `PORT`         | Crash/desync report upload                                             |
| `GET /metrics`                         | `METRICS_PORT` | Prometheus metrics                                                     |

## Running behind a reverse proxy (TLS)

For a public server you'll want `wss://` (TLS). Put the game port behind a reverse proxy that forwards websocket upgrades.

With nginx, forward the websocket upgrade and the client IP explicitly:

```nginx
location / {
    proxy_pass http://127.0.0.1:3000;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_read_timeout 1h;
}
```

## Ranked, leaderboard and profiles

The official TF.EX server runs this code plus a private ranking module

To add your own ranking, implement the `Ranking` trait
More info at [docs/RANKING.md](docs/RANKING.md)

## Development

```sh
cargo test
cargo run
```

## License

[GPL-3.0](LICENSE)

## Contact

Twitter : DShad - [@DShad66](https://twitter.com/DShad66)

Discord : dshad (was DShad#4670)

[contributors-shield]: https://img.shields.io/github/contributors/Fcornaire/tf-ex-matchmaking-server.svg?style=for-the-badge
[contributors-url]: https://github.com/Fcornaire/tf-ex-matchmaking-server/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/Fcornaire/tf-ex-matchmaking-server.svg?style=for-the-badge
[forks-url]: https://github.com/Fcornaire/tf-ex-matchmaking-server/network/members
[stars-shield]: https://img.shields.io/github/stars/Fcornaire/tf-ex-matchmaking-server.svg?style=for-the-badge
[stars-url]: https://github.com/Fcornaire/tf-ex-matchmaking-server/stargazers
[issues-shield]: https://img.shields.io/github/issues/Fcornaire/tf-ex-matchmaking-server.svg?style=for-the-badge
[issues-url]: https://github.com/Fcornaire/tf-ex-matchmaking-server/issues
[license-shield]: https://img.shields.io/github/license/Fcornaire/tf-ex-matchmaking-server.svg?style=for-the-badge
[download-shield]: https://img.shields.io/github/downloads/Fcornaire/tf-ex-matchmaking-server/total?style=for-the-badge
[download-url]: https://github.com/Fcornaire/tf-ex-matchmaking-server/releases
[license-url]: https://github.com/Fcornaire/tf-ex-matchmaking-server/blob/main/LICENSE
