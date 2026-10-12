/// client -> relay: `[PACKET_HELLO][token]` / `[PACKET_GAME][token][dst id][payload]`
/// relay -> client: `[PACKET_GAME][src id][payload]`

pub const PACKET_HELLO: u8 = 1;
pub const PACKET_GAME: u8 = 2;

const TOKEN_SIZE: usize = 8;

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
}
