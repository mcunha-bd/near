use anyhow::{Result, ensure};
use near_time::Duration;
use std::path::PathBuf;

fn default_database() -> String {
    "near_archive".to_owned()
}

fn default_table() -> String {
    "cold_rows".to_owned()
}

fn default_write_enabled() -> bool {
    true
}

fn default_connect_timeout() -> Duration {
    Duration::seconds(10)
}

fn default_request_timeout() -> Duration {
    Duration::minutes(5)
}

fn default_max_insert_rows() -> usize {
    10_000
}

fn default_max_insert_bytes() -> usize {
    32 * 1024 * 1024
}

/// Configuration for the ClickHouse cold archive backend.
///
/// The password is intentionally loaded from a file so it does not need to be
/// stored in `config.json`. Relative paths are resolved from the neard home.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ClickHouseArchiveConfig {
    /// ClickHouse HTTP endpoint, for example `https://clickhouse.example:8443`.
    pub endpoint: String,
    #[serde(default = "default_database")]
    pub database: String,
    #[serde(default = "default_table")]
    pub table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password_file: Option<PathBuf>,
    /// Serve cold misses through ClickHouse. Keep disabled during migration.
    #[serde(default)]
    pub read_enabled: bool,
    /// Copy finalized archival rows to ClickHouse.
    #[serde(default = "default_write_enabled")]
    pub write_enabled: bool,
    #[serde(default = "default_connect_timeout")]
    #[serde(with = "near_time::serde_duration_as_std")]
    pub connect_timeout: Duration,
    #[serde(default = "default_request_timeout")]
    #[serde(with = "near_time::serde_duration_as_std")]
    pub request_timeout: Duration,
    /// Maximum rows sent in one HTTP insert request.
    #[serde(default = "default_max_insert_rows")]
    pub max_insert_rows: usize,
    /// Approximate maximum RowBinary bytes sent in one HTTP insert request.
    #[serde(default = "default_max_insert_bytes")]
    pub max_insert_bytes: usize,
}

impl ClickHouseArchiveConfig {
    pub fn validate(&self) -> Result<()> {
        validate_identifier("database", &self.database)?;
        validate_identifier("table", &self.table)?;
        ensure!(
            self.read_enabled || self.write_enabled,
            "clickhouse archival must enable reads or writes"
        );
        ensure!(self.max_insert_rows > 0, "max_insert_rows must be greater than zero");
        ensure!(self.max_insert_bytes > 0, "max_insert_bytes must be greater than zero");
        Ok(())
    }
}

fn validate_identifier(kind: &str, identifier: &str) -> Result<()> {
    let mut chars = identifier.chars();
    let first = chars.next();
    let valid_first = first.is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic());
    let valid_rest = chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric());
    ensure!(valid_first && valid_rest, "invalid clickhouse {kind} identifier");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_sql_in_identifiers() {
        let config = ClickHouseArchiveConfig {
            endpoint: "http://localhost:8123".to_owned(),
            database: "near_archive; drop database near_archive".to_owned(),
            table: default_table(),
            username: None,
            password_file: None,
            read_enabled: false,
            write_enabled: true,
            connect_timeout: default_connect_timeout(),
            request_timeout: default_request_timeout(),
            max_insert_rows: default_max_insert_rows(),
            max_insert_bytes: default_max_insert_bytes(),
        };
        assert!(config.validate().is_err());
    }
}
