#[derive(Debug)]
pub struct Args {
    pub geoip_client_id: String,
    pub geoip_api_key: String,
}

impl Args {
    pub fn new(geoip_client_id: String, geoip_api_key: String) -> Self {
        Self {
            geoip_client_id,
            geoip_api_key,
        }
    }
}
