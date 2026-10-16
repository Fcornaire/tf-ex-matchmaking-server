/// Quick note for myself because i will forget those in 6 months (probably less)
/// client -> relay: `[PACKET_HELLO][token]` / `[PACKET_GAME][token][dst id][payload]`
/// relay -> client: `[PACKET_GAME][src id][payload]`
/// relay -> relay: `[PACKET_PORT_CHECK][check id][port]`

pub const PACKET_HELLO: u8 = 1;
pub const PACKET_GAME: u8 = 2;
pub const PACKET_PORT_CHECK: u8 = 3;

const TOKEN_SIZE: usize = 8;
const PORT_CHECK_ID_SIZE: usize = 8;

#[derive(Debug, PartialEq)]
pub enum Upstream<'a> {
    Hello {
        token: u64,
    },
    Data {
        token: u64,
        dst: u8,
        payload: &'a [u8],
    },
}

impl Upstream<'_> {
    pub fn token(&self) -> u64 {
        match self {
            Upstream::Hello { token } | Upstream::Data { token, .. } => *token,
        }
    }
}

pub fn parse_upstream(buffer: &[u8]) -> Option<Upstream<'_>> {
    let (&kind, rest) = buffer.split_first()?;
    let token = u64::from_le_bytes(rest.get(..TOKEN_SIZE)?.try_into().ok()?);
    let rest = &rest[TOKEN_SIZE..];

    match kind {
        PACKET_HELLO => Some(Upstream::Hello { token }),
        PACKET_GAME => {
            let (&dst, payload) = rest.split_first()?;

            Some(Upstream::Data {
                token,
                dst,
                payload,
            })
        }
        _ => None,
    }
}

pub fn write_port_check(id: u64, port: u16) -> Vec<u8> {
    let mut out = vec![PACKET_PORT_CHECK];
    out.extend_from_slice(&id.to_le_bytes());
    out.extend_from_slice(&port.to_le_bytes());
    out
}

pub fn parse_port_check(buffer: &[u8]) -> Option<(u64, u16)> {
    let (&kind, rest) = buffer.split_first()?;

    if kind != PACKET_PORT_CHECK || rest.len() != PORT_CHECK_ID_SIZE + 2 {
        return None;
    }

    let id = u64::from_le_bytes(rest[..PORT_CHECK_ID_SIZE].try_into().ok()?);
    let port = u16::from_le_bytes(rest[PORT_CHECK_ID_SIZE..].try_into().ok()?);

    Some((id, port))
}

pub fn write_downstream(out: &mut Vec<u8>, src: u8, payload: &[u8]) {
    out.clear();
    out.push(PACKET_GAME);
    out.push(src);
    out.extend_from_slice(payload);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn upstream(kind: u8, token: u64, tail: &[u8]) -> Vec<u8> {
        let mut datagram = vec![kind];
        datagram.extend_from_slice(&token.to_le_bytes());
        datagram.extend_from_slice(tail);
        datagram
    }

    #[test]
    fn parses_hello_and_data() {
        assert_eq!(
            parse_upstream(&upstream(PACKET_HELLO, 42, &[])),
            Some(Upstream::Hello { token: 42 })
        );
        assert_eq!(
            parse_upstream(&upstream(PACKET_GAME, 7, &[3, 9, 9])),
            Some(Upstream::Data {
                token: 7,
                dst: 3,
                payload: &[9, 9]
            })
        );
    }

    #[test]
    fn rejects_truncated_or_unknown() {
        assert_eq!(parse_upstream(&[]), None);
        assert_eq!(parse_upstream(&[PACKET_HELLO, 1, 2, 3]), None);
        assert_eq!(parse_upstream(&upstream(PACKET_GAME, 7, &[])), None);
        assert_eq!(parse_upstream(&upstream(9, 7, &[1])), None);
    }

    #[test]
    fn downstream_carries_source() {
        let mut out = vec![1, 2, 3];

        write_downstream(&mut out, 4, &[5, 6]);

        assert_eq!(out, vec![PACKET_GAME, 4, 5, 6]);
    }

    #[test]
    fn port_check_round_trips_and_is_never_upstream() {
        let packet = write_port_check(7, 4538);

        assert_eq!(parse_port_check(&packet), Some((7, 4538)));
        assert_eq!(parse_upstream(&packet), None);
        assert_eq!(parse_port_check(&[PACKET_PORT_CHECK, 1, 2]), None);
        assert_eq!(parse_port_check(&upstream(PACKET_HELLO, 7, &[0, 0])), None);
    }
}
