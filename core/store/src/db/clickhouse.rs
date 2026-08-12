use crate::archive::clickhouse::config::ClickHouseArchiveConfig;
use crate::db::refcount;
use crate::db::{DBIterator, DBOp, DBSlice, DBTransaction, Database};
use crate::{DBCol, StoreStatistics};
use anyhow::{Context, Error, Result, anyhow, bail, ensure};
use hex::encode as hex_encode;
use reqwest::StatusCode;
use reqwest::blocking::{Client, RequestBuilder};
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

#[derive(Clone)]
struct Row {
    key: Vec<u8>,
    value: Vec<u8>,
    deleted: bool,
}

/// A ClickHouse implementation of nearcore's raw key-value database contract.
///
/// Rows keep the exact RocksDB key and value bytes. This makes the backend a
/// lossless cold-store target and lets existing `Store` callers deserialize
/// values without ClickHouse-specific code.
pub struct ClickHouseDB {
    client: Client,
    config: ClickHouseArchiveConfig,
    chain_id: String,
    password: Option<String>,
    archive_height: AtomicU64,
    last_error: Mutex<Option<String>>,
}

impl ClickHouseDB {
    pub fn open(
        config: ClickHouseArchiveConfig,
        chain_id: String,
        home_dir: &Path,
    ) -> Result<Self> {
        config.validate()?;
        let password = config
            .password_file
            .as_ref()
            .map(|path| {
                let path = if path.is_absolute() { path.clone() } else { home_dir.join(path) };
                fs::read_to_string(&path)
                    .with_context(|| {
                        format!("failed to read clickhouse password file {}", path.display())
                    })
                    .map(|password| password.trim_end().to_owned())
            })
            .transpose()?;
        let client = Client::builder()
            .connect_timeout(config.connect_timeout.unsigned_abs())
            .timeout(config.request_timeout.unsigned_abs())
            .build()
            .context("failed to build clickhouse http client")?;
        let database = Self {
            client,
            config,
            chain_id,
            password,
            archive_height: AtomicU64::new(0),
            last_error: Mutex::new(None),
        };
        database.check_schema()?;
        Ok(database)
    }

    pub fn read_enabled(&self) -> bool {
        self.config.read_enabled
    }

    pub fn write_enabled(&self) -> bool {
        self.config.write_enabled
    }

    pub fn begin_archive_height(&self, height: u64) {
        self.archive_height.store(height, Ordering::Relaxed);
        *self.lock_error() = None;
    }

    pub fn take_last_error(&self) -> Option<String> {
        self.lock_error().take()
    }

    fn lock_error(&self) -> MutexGuard<'_, Option<String>> {
        self.last_error.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn record_error(&self, error: &Error) {
        let mut slot = self.lock_error();
        if slot.is_none() {
            *slot = Some(format!("{error:#}"));
        }
        tracing::error!(target: "clickhouse_archive", ?error, "clickhouse archive operation failed");
    }

    fn qualified_table(&self) -> String {
        format!("{}.{}", self.config.database, self.config.table)
    }

    fn request(&self, query: &str) -> RequestBuilder {
        let request = self.client.post(&self.config.endpoint).query(&[
            ("database", self.config.database.as_str()),
            ("query", query),
            ("wait_end_of_query", "1"),
        ]);
        match &self.config.username {
            Some(username) => request.basic_auth(username, self.password.as_ref()),
            None => request,
        }
    }

    fn execute(&self, query: &str, parameters: &[(&str, String)]) -> Result<Vec<u8>> {
        let mut request = self.request(query);
        for (name, value) in parameters {
            request = request.query(&[(format!("param_{name}"), value)]);
        }
        let response = request.send().context("clickhouse request failed")?;
        let status = response.status();
        let body = response.bytes().context("failed to read clickhouse response")?;
        if status != StatusCode::OK {
            bail!(
                "clickhouse returned {status}: {}",
                String::from_utf8_lossy(body.as_ref()).trim()
            );
        }
        Ok(body.to_vec())
    }

    fn check_schema(&self) -> Result<()> {
        let query = format!(
            "SELECT count() FROM system.columns WHERE database = {{database:String}} AND table = {{table:String}} AND name IN ('chain_id', 'column_name', 'row_key', 'row_value', 'archive_height', 'is_deleted') FORMAT RowBinary"
        );
        let body = self.execute(
            &query,
            &[("database", self.config.database.clone()), ("table", self.config.table.clone())],
        )?;
        let count = decode_u64(&body).context("invalid clickhouse schema check response")?;
        ensure!(
            count == 6,
            "clickhouse archive table {} is missing or has an incompatible schema",
            self.qualified_table()
        );
        Ok(())
    }

    fn write_rows(&self, rows: &[RowWithColumn]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let query = format!(
            "INSERT INTO {} (chain_id, column_name, row_key, row_value, archive_height, is_deleted) FORMAT RowBinary",
            self.qualified_table()
        );
        let mut body = Vec::new();
        let mut row_count = 0usize;
        for row in rows {
            encode_string(&mut body, self.chain_id.as_bytes());
            encode_string(&mut body, row.column.as_bytes());
            encode_string(&mut body, &row.row.key);
            encode_string(&mut body, &row.row.value);
            body.extend_from_slice(&self.archive_height.load(Ordering::Relaxed).to_le_bytes());
            body.push(u8::from(row.row.deleted));
            row_count += 1;
            if row_count >= self.config.max_insert_rows
                || body.len() >= self.config.max_insert_bytes
            {
                self.send_insert(&query, body)?;
                body = Vec::new();
                row_count = 0;
            }
        }
        if !body.is_empty() {
            self.send_insert(&query, body)?;
        }
        Ok(())
    }

    fn send_insert(&self, query: &str, body: Vec<u8>) -> Result<()> {
        let response = self.request(query).body(body).send().context("clickhouse insert failed")?;
        let status = response.status();
        let response_body =
            response.bytes().context("failed to read clickhouse insert response")?;
        if status != StatusCode::OK {
            bail!(
                "clickhouse insert returned {status}: {}",
                String::from_utf8_lossy(response_body.as_ref()).trim()
            );
        }
        Ok(())
    }

    fn read_one(&self, col: DBCol, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let query = format!(
            "SELECT row_value, is_deleted FROM {} WHERE chain_id = {{chain_id:String}} AND column_name = {{column_name:String}} AND row_key = unhex({{key_hex:String}}) ORDER BY archive_height DESC LIMIT 1 FORMAT RowBinary",
            self.qualified_table()
        );
        let body = self.execute(
            &query,
            &[
                ("chain_id", self.chain_id.clone()),
                ("column_name", column_name(col).to_owned()),
                ("key_hex", hex_encode(key)),
            ],
        )?;
        if body.is_empty() {
            return Ok(None);
        }
        let mut cursor = 0;
        let value = decode_string(&body, &mut cursor)?;
        let deleted = decode_byte(&body, &mut cursor)? != 0;
        Ok((!deleted).then_some(value))
    }

    fn read_rows(
        &self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
        prefix: Option<&[u8]>,
        strip_refcount: bool,
    ) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>> {
        let mut filters = Vec::new();
        let mut parameters =
            vec![("chain_id", self.chain_id.clone()), ("column_name", column_name(col).to_owned())];
        if let Some(lower_bound) = lower_bound {
            filters.push("row_key >= unhex({lower_hex:String})");
            parameters.push(("lower_hex", hex_encode(lower_bound)));
        }
        if let Some(upper_bound) = upper_bound {
            filters.push("row_key < unhex({upper_hex:String})");
            parameters.push(("upper_hex", hex_encode(upper_bound)));
        }
        if let Some(prefix) = prefix {
            filters.push("startsWith(row_key, unhex({prefix_hex:String}))");
            parameters.push(("prefix_hex", hex_encode(prefix)));
        }
        let extra_filter = if filters.is_empty() {
            String::new()
        } else {
            format!(" AND {}", filters.join(" AND "))
        };
        let query = format!(
            "SELECT row_key, argMax(row_value, archive_height), argMax(is_deleted, archive_height) FROM {} WHERE chain_id = {{chain_id:String}} AND column_name = {{column_name:String}}{} GROUP BY row_key HAVING argMax(is_deleted, archive_height) = 0 ORDER BY row_key FORMAT RowBinary",
            self.qualified_table(),
            extra_filter
        );
        let body = self.execute(&query, &parameters)?;
        let mut cursor = 0;
        let mut result = Vec::new();
        while cursor < body.len() {
            let key = decode_string(&body, &mut cursor)?;
            let value = decode_string(&body, &mut cursor)?;
            let _deleted = decode_byte(&body, &mut cursor)?;
            let value = if strip_refcount && col.is_rc() {
                match refcount::strip_refcount(value) {
                    Some(value) => value,
                    None => continue,
                }
            } else {
                value
            };
            result.push((key.into_boxed_slice(), value.into_boxed_slice()));
        }
        Ok(result)
    }

    fn iterator_or_empty(&self, result: Result<Vec<(Box<[u8]>, Box<[u8]>)>>) -> DBIterator<'_> {
        match result {
            Ok(rows) => Box::new(rows.into_iter()),
            Err(error) => {
                self.record_error(&error);
                panic!("clickhouse archive read failed: {error:#}")
            }
        }
    }
}

struct RowWithColumn {
    column: &'static str,
    row: Row,
}

impl Database for ClickHouseDB {
    fn get_raw_bytes(&self, col: DBCol, key: &[u8]) -> Option<DBSlice<'_>> {
        match self.read_one(col, key) {
            Ok(value) => value.map(DBSlice::from_vec),
            Err(error) => {
                self.record_error(&error);
                panic!("clickhouse archive read failed: {error:#}")
            }
        }
    }

    fn iter<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.iterator_or_empty(self.read_rows(col, None, None, None, true))
    }

    fn iter_prefix<'a>(&'a self, col: DBCol, key_prefix: &'a [u8]) -> DBIterator<'a> {
        self.iterator_or_empty(self.read_rows(col, None, None, Some(key_prefix), true))
    }

    fn iter_range<'a>(
        &'a self,
        col: DBCol,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
    ) -> DBIterator<'a> {
        self.iterator_or_empty(self.read_rows(col, lower_bound, upper_bound, None, true))
    }

    fn iter_raw_bytes<'a>(&'a self, col: DBCol) -> DBIterator<'a> {
        self.iterator_or_empty(self.read_rows(col, None, None, None, false))
    }

    fn write(&self, batch: DBTransaction) {
        if self.lock_error().is_some() {
            return;
        }
        if !self.write_enabled() {
            let error = anyhow!("clickhouse archive writes are disabled");
            self.record_error(&error);
            return;
        }
        let mut rows = Vec::with_capacity(batch.ops.len());
        for operation in batch.ops {
            let (col, row) = match operation {
                DBOp::Set { col, key, value }
                | DBOp::Insert { col, key, value }
                | DBOp::UpdateRefcount { col, key, value } => {
                    (col, Row { key, value, deleted: false })
                }
                DBOp::Delete { col, key } => (col, Row { key, value: Vec::new(), deleted: true }),
                DBOp::DeleteAll { .. } | DBOp::DeleteRange { .. } => {
                    let error = anyhow!("bulk deletes are not supported by clickhouse archive");
                    self.record_error(&error);
                    return;
                }
            };
            rows.push(RowWithColumn { column: column_name(col), row });
        }
        if let Err(error) = self.write_rows(&rows) {
            self.record_error(&error);
        }
    }

    fn flush(&self) {}

    fn compact(&self) {}

    fn get_store_statistics(&self) -> Option<StoreStatistics> {
        None
    }

    fn create_checkpoint(&self, _path: &Path, _columns_to_keep: Option<&[DBCol]>) -> Result<()> {
        bail!("clickhouse archive does not support local checkpoints")
    }
}

fn column_name(col: DBCol) -> &'static str {
    col.into()
}

fn encode_string(output: &mut Vec<u8>, value: &[u8]) {
    encode_var_uint(output, value.len() as u64);
    output.extend_from_slice(value);
}

fn encode_var_uint(output: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            return;
        }
    }
}

fn decode_u64(input: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = input.try_into().context("expected one rowbinary uint64")?;
    Ok(u64::from_le_bytes(bytes))
}

fn decode_byte(input: &[u8], cursor: &mut usize) -> Result<u8> {
    let value = *input.get(*cursor).context("truncated rowbinary byte")?;
    *cursor += 1;
    Ok(value)
}

fn decode_string(input: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let length = decode_var_uint(input, cursor)?;
    let length = usize::try_from(length).context("rowbinary string length does not fit usize")?;
    let end = cursor.checked_add(length).context("rowbinary string length overflow")?;
    let value = input.get(*cursor..end).context("truncated rowbinary string")?.to_vec();
    *cursor = end;
    Ok(value)
}

fn decode_var_uint(input: &[u8], cursor: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    for shift in (0..=63).step_by(7) {
        let byte = decode_byte(input, cursor)?;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    bail!("invalid rowbinary varuint")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rowbinary_string_roundtrip() {
        let value = vec![0, 1, 127, 128, 255];
        let mut encoded = Vec::new();
        encode_string(&mut encoded, &value);
        let mut cursor = 0;
        assert_eq!(decode_string(&encoded, &mut cursor).unwrap(), value);
        assert_eq!(cursor, encoded.len());
    }

    #[test]
    fn rowbinary_long_string_roundtrip() {
        let value = vec![42; 16_384];
        let mut encoded = Vec::new();
        encode_string(&mut encoded, &value);
        let mut cursor = 0;
        assert_eq!(decode_string(&encoded, &mut cursor).unwrap(), value);
    }
}
