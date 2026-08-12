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
    "bootstrap_from_local_cold_store": true,
    "connect_timeout": { "secs": 10, "nanos": 0 },
    "request_timeout": { "secs": 300, "nanos": 0 },
    "max_insert_rows": 10000,
    "max_insert_bytes": 33554432
  }
}
```

On a legacy archive, the copier reads history from the archival RocksDB by
finalized height. On an existing split archive, the local RocksDB cold writer
remains authoritative while a background ClickHouse actor bulk-streams it.
It records the local cold head `H0`, checkpoints each bounded insert batch in
ClickHouse, writes `HEAD` and `COLD_HEAD` as `H0` last, and only then enables
the ClickHouse finalized-height tail. A restart resumes at the next key in the
unfinished column for the same `H0`. This serializes ClickHouse writes while
the node continues syncing and writing its local cold store. Rows written
locally after `H0` can be replayed safely.

Set `bootstrap_from_local_cold_store` to `false` only for a preloaded target or
to replay the ClickHouse tail from genesis. For a disposable target, drop and
recreate the configured table, leave this setting enabled, and restart neard;
an empty table has no `HEAD`, so bootstrap starts again automatically.

Monitor `near_archive.ingest_progress`, ClickHouse rejected-query logs, disk
free space, merge backlog, and the existing nearcore cold-copy result metric.
Validate random keys and height windows against RocksDB before changing
`read_enabled` to `true`. Once reads are enabled, nearcore uses hot RocksDB
first and ClickHouse only for cold misses. Keep `write_enabled` on after cutover.

The `ReplacingMergeTree` model intentionally tolerates retried inserts. Duplicate
physical rows disappear during merges; nearcore reads use `argMax` or an ordered
point lookup and do not require `FINAL`. The high ZSTD level on `row_value` is a
deliberate storage-first choice and should be tuned only after corpus benchmarks.
