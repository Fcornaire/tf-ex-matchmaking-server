pub trait StrExts {
    fn is_an_ip_and_private(&self) -> bool;
}

impl StrExts for str {
    fn is_an_ip_and_private(&self) -> bool {
        let ip = match self.parse::<std::net::IpAddr>() {
            Ok(ip) => ip,
            Err(_) => return false,
        };

        match ip {
            std::net::IpAddr::V4(ipv4) => ipv4.is_private(),
            std::net::IpAddr::V6(_) => false,
        }
    }
}
