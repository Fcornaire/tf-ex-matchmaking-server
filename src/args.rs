#[derive(Debug)]
pub struct Args {
    pub geoip_client_id: Option<String>,
    pub geoip_api_key: Option<String>,
}

impl Args {
    pub fn new(geoip_client_id: Option<String>, geoip_api_key: Option<String>) -> Self {
        Self {
            geoip_client_id,
            geoip_api_key,
        }
    }

    pub fn geoip_credentials(&self) -> Option<(String, String)> {
        match (&self.geoip_client_id, &self.geoip_api_key) {
            (Some(id), Some(key)) => Some((id.clone(), key.clone())),
            _ => None,
        }
    }
}
