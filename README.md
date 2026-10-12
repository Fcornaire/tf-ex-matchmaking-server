# tf-matchmaking-server

Matchmaking and WebRTC signaling server for **TF.EX**, the TowerFall Ascension netplay mod.

The server only handles matchmaking and connection setup, actual gameplay traffic is peer-to-peer between players (WebRTC data channels, STUN-only NAT traversal).

## What it does

- **Lobby browser** — public lobbies players can create, join, and spectate
- **Private lobbies** — joinable by 5-character code
- **Quickplay** — a queue that matches players automatically, ranking candidates by geographic proximity when GeoIP is configured
- **Keep-alive RTT tracking** — feeds the in-game automatic input delay suggestion
- **Prometheus metrics** — optional, served on a separate port

## Quick start

No configuration is required — the server runs out of the box:

```sh
cargo run --release
```

Or with Docker:

```sh
docker build -t tf-matchmaking-server .
docker run -p 3000:3000 tf-matchmaking-server
```

## Configuration

Everything is configured through environment variables. All of them are optional:

| Variable          | Default   | Purpose                                               |
| ----------------- | --------- | ----------------------------------------------------- |
| `PORT`            | `3000`    | Game-facing port (matchmaking + signaling websockets) |
| `METRICS_PORT`    | `4738`    | Prometheus metrics port (keep it private)             |
| `GEOIP_CLIENT_ID` | _(unset)_ | MaxMind account ID — enables GeoIP                    |
| `GEOIP_API_KEY`   | _(unset)_ | MaxMind license key — enables GeoIP                   |
| `RELAY_PORT`      | `4537`    | UDP port of the relay                                 |

## Relay (optional)

Players normally connect to each other directly (WebRTC). Some pairs can't, behind strict NATs (or weird network stuff). A lobby on the relay sends its game traffic through the server instead

Before any match, every pair of players must measure its ping (10 s each)

Spectators try both paths and keep the relay only when they can't reach the host directly

## GeoIP (optional — but recommended if you want quickplay by region)

Quickplay ranks candidate opponents by geographic distance. That distance comes from a GeoIP lookup of each connecting player's IP.

**Without GeoIP** the server still runs fine and quickplay still works but it just treats every player as same distance, so matches are made with no regard for region or ping.

**To enable it**, create a free [MaxMind GeoLite2 account](https://www.maxmind.com/en/geolite2/signup), generate a license key, and set:

```sh
GEOIP_CLIENT_ID=<your MaxMind account ID>
GEOIP_API_KEY=<your MaxMind license key>
```

The server logs `GeoIP enabled` / `GeoIP disabled` at startup so you can confirm the configuration.

Notes:

- Lookups use MaxMind's hosted GeoLite2 City web service (`geolite.info`) — no database download needed. The free tier's daily quota is far beyond what a game server generates (one lookup per connection).
- Behind a reverse proxy, the server reads `X-Forwarded-For` to geolocate the real client IP, use a reverse proxy like nginx.
- Failed lookups fall back to a placeholder location rather than rejecting the player.

## Metrics (optional)

A Prometheus endpoint is served on `METRICS_PORT` (default `4738`) at `/metrics`.

Scrape config:

```yaml
scrape_configs:
  - job_name: tf-matchmaking
    static_configs:
      - targets: ["127.0.0.1:4738"]
```

All metrics are prefixed `tfex_`.

## Endpoints

| Endpoint                               | Port           | Purpose                                                                |
| -------------------------------------- | -------------- | ---------------------------------------------------------------------- |
| `GET /ws`                              | `PORT`         | Matchmaking websocket (lobby browser, quickplay, chat with the server) |
| `GET /room/:room_id`                   | `PORT`         | WebRTC signaling room for one lobby                                    |
| `GET /relay/room/:room_id`             | `PORT`         | Relay connection for one lobby (data over UDP `RELAY_PORT`)            |
| `GET /relay/ping_measurement/:room_id` | `PORT`         | Lobby ping measurement through the relay                               |
| `GET /metrics`                         | `METRICS_PORT` | Prometheus metrics                                                     |

## Running behind a reverse proxy (TLS)

For a public server you'll want `wss://` (TLS). Put the game port behind a reverse proxy that forwards websocket upgrades; the two routes above are all it needs.

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

## Pointing the game at your server (TODO need update)

The TF.EX client's server address is set at build time in `src/network/TF.EX.Domain/Config.cs`:

```csharp
public static readonly string SERVER = "ws://127.0.0.1:3000";
```

Set it to your server and rebuild the mod.

## Development

```sh
cargo test
cargo run
```

## License

[GPL-3.0](LICENSE)
