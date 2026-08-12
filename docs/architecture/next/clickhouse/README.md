# ClickHouse archive provisioning

This directory contains the first migration phase: a lossless ClickHouse model
that preserves every cold RocksDB key and value byte. nearcore reads and writes
this table through its existing `Database`, `ColdDB`, `Store`, and `SplitDB`
abstractions, so Borsh encodings and lookup keys remain unchanged.

## Provision

1. Mount the NVMe path and each cold HDD/NVMe path shown in
   `storage_configuration.xml`, adjusting paths and disk count for the host.
2. Merge that file into the ClickHouse server configuration and restart the
   server. Confirm `near_archive_tiered` appears in `system.storage_policies`.
3. Apply `001_archive.sql` with `clickhouse-client --multiquery`.
4. Create a least-privilege account allowed to `SELECT` and `INSERT` on
   `near_archive.cold_rows` and to `SELECT` from `system.columns`.
5. Store its password in a mode-0600 file under the neard home directory.

Example `config.json` fragment for dual-writing while RocksDB remains the read
source:

```json
{
  "archive": true,
  "save_trie_changes": true,
  "clickhouse_archival": {
    "endpoint": "https://clickhouse.example.net:8443",
    "database": "near_archive",
    "table": "cold_rows",
    "username": "nearcore_archive",
    "password_file": "clickhouse-password",
    "read_enabled": false,
    "write_enabled": true,
    "connect_timeout": { "secs": 10, "nanos": 0 },
    "request_timeout": { "secs": 300, "nanos": 0 },
    "max_insert_rows": 10000,
    "max_insert_bytes": 33554432
  }
}
```

On a legacy archive, the copier reads history from the archival RocksDB by
finalized height. On an existing split archive, startup first bulk-streams the
completed local cold RocksDB snapshot, writes its head marker last, and then
resumes finalized-height copying through local hot+cold RocksDB. ClickHouse
stores its own `COLD_HEAD`, making restarts resumable and independent of local
cold-store progress.

Monitor `near_archive.ingest_progress`, ClickHouse rejected-query logs, disk
free space, merge backlog, and the existing nearcore cold-copy result metric.
Validate random keys and height windows against RocksDB before changing
`read_enabled` to `true`. Once reads are enabled, nearcore uses hot RocksDB
first and ClickHouse only for cold misses. Keep `write_enabled` on after cutover.

The `ReplacingMergeTree` model intentionally tolerates retried inserts. Duplicate
physical rows disappear during merges; nearcore reads use `argMax` or an ordered
point lookup and do not require `FINAL`. The high ZSTD level on `row_value` is a
deliberate storage-first choice and should be tuned only after corpus benchmarks.
