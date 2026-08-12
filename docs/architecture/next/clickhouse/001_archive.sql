-- Raw parity model consumed directly by nearcore's ClickHouseDB backend.
-- Change the database name before applying if config.json uses another name.

CREATE DATABASE IF NOT EXISTS near_archive;

CREATE TABLE IF NOT EXISTS near_archive.cold_rows
(
    chain_id LowCardinality(String) CODEC(ZSTD(3)),
    column_name LowCardinality(String) CODEC(ZSTD(3)),
    row_key String CODEC(ZSTD(6)),
    row_value String CODEC(ZSTD(12)),
    archive_height UInt64 CODEC(T64, ZSTD(3)),
    is_deleted UInt8 CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(archive_height)
PARTITION BY (chain_id, column_name)
ORDER BY (chain_id, column_name, row_key)
SETTINGS
    storage_policy = 'near_archive_tiered',
    index_granularity = 8192,
    min_bytes_for_wide_part = 1073741824,
    min_rows_for_wide_part = 1000000;

-- Operational model: ingestion coverage by NEAR column. archive_height is the
-- finalized block height assigned by the nearcore copier.
CREATE VIEW IF NOT EXISTS near_archive.ingest_progress AS
SELECT
    chain_id,
    column_name,
    max(archive_height) AS highest_archive_height,
    count() AS physical_rows,
    formatReadableSize(sum(length(row_key) + length(row_value))) AS uncompressed_payload
FROM near_archive.cold_rows
GROUP BY chain_id, column_name;

-- Logical live-row model for audits and ad-hoc queries. nearcore uses more
-- selective parameterized queries directly against cold_rows so primary-key
-- pruning happens before aggregation.
CREATE VIEW IF NOT EXISTS near_archive.latest_rows AS
SELECT
    chain_id,
    column_name,
    row_key,
    argMax(row_value, archive_height) AS row_value,
    max(archive_height) AS archive_height
FROM near_archive.cold_rows
GROUP BY chain_id, column_name, row_key
HAVING argMax(is_deleted, archive_height) = 0;
