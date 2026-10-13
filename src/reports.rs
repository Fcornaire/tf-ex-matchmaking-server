use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Bytes,
    extract::{Query, State},
    http::StatusCode,
};
use serde::Deserialize;
use tracing::{error, info, warn};

use crate::state::AppState;

const MAX_NAME_LEN: usize = 48;
const MAX_GROUP_LEN: usize = 200;

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReportKind {
    Desync,
    Disconnect,
    Crash,
}

impl ReportKind {
    fn as_str(self) -> &'static str {
        match self {
            ReportKind::Desync => "desync",
            ReportKind::Disconnect => "disconnect",
            ReportKind::Crash => "crash",
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ReportParams {
    group: String,
    name: String,
    kind: ReportKind,
}

#[derive(Debug)]
pub struct Reports {
    dir: PathBuf,
    max_bytes: u64,
}

#[derive(Debug)]
pub enum SaveError {
    Full,
    Io(io::Error),
}

impl Reports {
    pub fn open(dir: impl Into<PathBuf>, max_bytes: u64) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        let test_write = dir.join(".test");
        fs::write(&test_write, b"")?;
        fs::remove_file(&test_write)?;

        Ok(Self { dir, max_bytes })
    }

    pub fn save(
        &self,
        group: &str,
        name: &str,
        kind: ReportKind,
        bytes: &[u8],
    ) -> Result<PathBuf, SaveError> {
        self.save_at(group, name, kind, bytes, SystemTime::now())
    }

    fn save_at(
        &self,
        group: &str,
        name: &str,
        kind: ReportKind,
        bytes: &[u8],
        time: SystemTime,
    ) -> Result<PathBuf, SaveError> {
        if total_size(&self.dir).map_err(SaveError::Io)? + bytes.len() as u64 > self.max_bytes {
            return Err(SaveError::Full);
        }

        let group_dir = self.dir.join(group);
        fs::create_dir_all(&group_dir).map_err(SaveError::Io)?;

        let final_name = format!("{}_{}_{}", name, kind.as_str(), utc_hhmmss(time));

        for attempt in 1..=100 {
            let file_name = if attempt == 1 {
                format!("{final_name}.zip")
            } else {
                format!("{final_name}_{attempt}.zip")
            };
            let path = group_dir.join(file_name);

            match fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut file) => {
                    if let Err(err) = file.write_all(bytes) {
                        let _ = fs::remove_file(&path);
                        return Err(SaveError::Io(err));
                    }

                    return Ok(path);
                }
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(err) => return Err(SaveError::Io(err)),
            }
        }

        Err(SaveError::Io(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "no free report name",
        )))
    }

    pub fn purge_older_than(&self, max_age: Duration) -> io::Result<usize> {
        let now = SystemTime::now();
        let mut removed = 0;

        for group in fs::read_dir(&self.dir)? {
            let group = group?.path();
            if !group.is_dir() {
                continue;
            }

            for file in fs::read_dir(&group)? {
                let file = file?;
                let age = file
                    .metadata()?
                    .modified()
                    .ok()
                    .and_then(|modified| now.duration_since(modified).ok())
                    .unwrap_or_default();

                if age > max_age {
                    fs::remove_file(file.path())?;
                    removed += 1;
                }
            }

            if fs::read_dir(&group)?.next().is_none() {
                fs::remove_dir(&group)?;
            }
        }

        Ok(removed)
    }
}

fn total_size(dir: &Path) -> io::Result<u64> {
    let mut total = 0;

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;

        if metadata.is_dir() {
            for file in fs::read_dir(entry.path())? {
                total += file?.metadata()?.len();
            }
        } else {
            total += metadata.len();
        }
    }

    Ok(total)
}

fn utc_hhmmss(time: SystemTime) -> String {
    let secs = time
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
        % 86_400;

    format!("{:02}{:02}{:02}", secs / 3600, secs / 60 % 60, secs % 60)
}

pub fn sanitize(value: &str, max_len: usize) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(max_len)
        .collect()
}

pub async fn report_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ReportParams>,
    body: Bytes,
) -> StatusCode {
    let Some(reports) = state.reports() else {
        return StatusCode::NOT_FOUND;
    };

    let group = sanitize(&params.group, MAX_GROUP_LEN);
    let name = sanitize(&params.name, MAX_NAME_LEN);
    let kind = params.kind;

    if group.is_empty() || name.is_empty() || body.is_empty() {
        return StatusCode::BAD_REQUEST;
    }

    let size_kb = body.len() / 1024;

    match tokio::task::spawn_blocking(move || reports.save(&group, &name, kind, &body)).await {
        Ok(Ok(path)) => {
            info!("Report saved to {} ({} KB)", path.display(), size_kb);
            StatusCode::CREATED
        }
        Ok(Err(SaveError::Full)) => {
            warn!(
                "Report refused ({} KB), the reports folder is full",
                size_kb
            );
            StatusCode::INSUFFICIENT_STORAGE
        }
        Ok(Err(SaveError::Io(err))) => {
            error!("Report not saved: {}", err);
            StatusCode::INTERNAL_SERVER_ERROR
        }
        Err(err) => {
            error!("Report not saved: {}", err);
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
