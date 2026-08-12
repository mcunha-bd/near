# NEAR ClickHouse archive storage: compress the history without hiding the lookup cost

Status: exploration

Date: 2026-08-11

nearcore revision reviewed: `82983fe95e9a`

## Context and scope

An archive node currently keeps the complete canonical history in RocksDB. Split storage moves
garbage-collectable historical rows from the hot database to a cold RocksDB database, while
`SplitDB` preserves the logical key/value view by reading hot first and cold second. The reported
size of the cold database is approximately 110 TiB. That number is an operator-provided estimate,
not a measurement made as part of this exploration.

The concrete problem is not only to compress those bytes. nearcore still expects bounded lookups
for blocks, chunks, transactions, receipts, outcomes, state changes, and content-addressed trie
objects. Putting height-ordered history in ClickHouse without a hash catalogue would make the common
transaction lookup scan history, which is operationally unusable even if the compressed database is
small.

This design proposes a ClickHouse-backed archive in which:

- Mutable consensus state and compact permanent indexes remain in the hot RocksDB.
- Immutable archive payloads are grouped by block height and shard for compression.
- A compact hash-ordered locator catalogue remains on NVMe.
- Old payload parts are recompressed and moved by ClickHouse to a cold HDD or NVMe volume.
- An embedded nearcore archive reader resolves locators asynchronously and hydrates a bounded local
  cache, preserving existing synchronous `Store` consumers.

The design follows the existing split-storage boundary and reuses the versioned block, shard, and
epoch grouping being developed for cloud archival. It does not propose replacing RocksDB for block
production, consensus, current state, flat state, or other mutable node-local data.

In scope:

- The current hot/cold storage contract and archival `DBCol` rows.
- Required nearcore and indexer read patterns.
- A compression-first ClickHouse logical and physical model.
- The nearcore read/write integration boundary.
- Migration, validation, rollback, and benchmark gates.

Out of scope:

- Implementing the ClickHouse writer, reader, or schema in this change.
- Changing the NEAR protocol or Borsh wire formats.
- Updating the protocol schema or OpenAPI specification.
- Designing a general-purpose blockchain explorer schema.
- Promising a final compressed size before measuring the existing 110 TiB by column and data era.

## Goals and non-goals

### Goals

- Reduce the single-copy physical size of the existing cold archive materially. The measurement
  phase must establish the target; a design that saves only a few percent does not justify the new
  operational system.
- Keep every required nearcore archival lookup bounded by a sparse primary-key search, a compact
  locator lookup, or a bounded height/shard range. No required lookup may scan the full history.
- Preserve byte-level or semantic compatibility with the `Store` values consumed by nearcore.
- Keep ingestion append-only and behind finality so normal operation does not require ClickHouse
  updates or deletes.
- Let ClickHouse manage hot-to-cold placement and cold recompression across explicitly configured
  volumes.
- Make migration resumable and let operators return to the RocksDB cold store without replaying the
  chain.

### Non-goals

- Matching RocksDB point-read latency. Cold reads may be slower, provided they remain practical and
  do not stall consensus or the client actor.
- Serving current state from ClickHouse. The current trie, flat state, state-sync working sets, and
  consensus metadata remain RocksDB responsibilities.
- Indexing every account, action argument, contract key, or receipt edge in the first version.
  Those indexes duplicate high-cardinality data and should be justified by measured query demand.
- Using ClickHouse as a transactionally consistent replacement for all `Database` operations.
  MergeTree is an append-oriented analytical engine, not a general read/write key/value engine.
- Treating a multi-disk ClickHouse volume as redundancy. Disk placement and failure tolerance are
  separate decisions.

## The current boundary is already useful

### Code map

| Area | Relevant code | Consequence for this design |
| --- | --- | --- |
| Logical columns | [`core/store/src/columns.rs`](../../../core/store/src/columns.rs#L22) | `DBCol`, GC policy, cold eligibility, and key components are the compatibility contract. |
| Database interface | [`core/store/src/db/mod.rs`](../../../core/store/src/db/mod.rs#L195) | Reads and iterators are synchronous; a remote query cannot be introduced invisibly without blocking or caching. |
| Cold semantics | [`core/store/src/db/colddb.rs`](../../../core/store/src/db/colddb.rs#L16) | Cold rows are immutable, deletes are ignored, and reference-counted values are exposed with a synthetic count of one. |
| Hot/cold routing | [`core/store/src/db/splitdb.rs`](../../../core/store/src/db/splitdb.rs#L18) | Cold-capable columns read hot first, then cold; iterators merge both stores. |
| Store composition | [`core/store/src/node_storage/mod.rs`](../../../core/store/src/node_storage/mod.rs#L40) | The client uses hot storage, while the view client uses the split view. This keeps cold latency off consensus paths. |
| Continuous copy | [`core/store/src/archive/cold_storage.rs`](../../../core/store/src/archive/cold_storage.rs#L68) | The cold loop discovers all keys associated with one final height and copies columns in parallel. This is the natural ClickHouse ingest unit. |
| Cold actor | [`chain/client/src/archive/cold_store_actor.rs`](../../../chain/client/src/archive/cold_store_actor.rs#L91) | The writer advances from `COLD_HEAD` to `FINAL_HEAD`, including genesis and resharding handling. |
| Typed chain reads | [`core/store/src/adapter/chain_store.rs`](../../../core/store/src/adapter/chain_store.rs#L28) | Most required reads already have typed methods, which is a safer integration boundary than arbitrary SQL. |
| Higher-level reads | [`chain/chain/src/store/mod.rs`](../../../chain/chain/src/store/mod.rs#L537) | Outcome enumeration and state-change reads combine point lookups with prefix scans. |
| State-change keys | [`core/store/src/trie/shard_tries.rs`](../../../core/store/src/trie/shard_tries.rs#L922) | State changes are keyed by `block_hash || trie_key`; delayed receipts append `ShardUId`. |
| Cloud block envelope | [`core/store/src/archive/cloud_storage/blocks.rs`](../../../core/store/src/archive/cloud_storage/blocks.rs#L15) | `BlockData` already groups a block, `BlockInfo`, next hash, and optional chunk producers by height. |
| Cloud shard envelope | [`core/store/src/archive/cloud_storage/shards.rs`](../../../core/store/src/archive/cloud_storage/shards.rs#L31) | `ShardData` already distinguishes new and carried chunks and groups receipts, outcomes, changes, and inverse changes. |
| Cloud epoch envelope | [`core/store/src/archive/cloud_storage/epoch_data.rs`](../../../core/store/src/archive/cloud_storage/epoch_data.rs#L15) | Small epoch metadata is sufficient to interpret shard layouts and bootstrap a range. |
| Cloud reader | [`chain/client/src/archive/cloud_archival_reader.rs`](../../../chain/client/src/archive/cloud_archival_reader.rs) | The current reader downloads batches and materializes normal `DBCol` rows locally. ClickHouse should follow this precedent instead of making all callers SQL-aware. |

### Which columns are archival

`DBCol::is_cold()` is explicit and intentionally requires a decision whenever a new column is
added. At the reviewed revision, the cold set is:

| Family | Cold columns | Notes |
| --- | --- | --- |
| Blocks and indexes | `Block`, `BlockInfo`, `BlockPerHeight`, `NextBlockHashes`, `ChunkHashesByHeight` | `BlockHeader` becomes cold only when `ContinuousEpochSync` is enabled. `BlockHeight` remains permanent in hot RocksDB. |
| Chunks and shard execution | `Chunks`, `ChunkExtra`, `IncomingReceipts`, `OutgoingReceipts`, `StateHeaders`, `StateChanges`, `StateChangesForSplitStates`, `ChunkApplyStats` | Most keys begin with a block hash or identify one chunk. |
| Transaction and receipt objects | `Transactions`, `Receipts`, `ReceiptToTx` | `Transactions` and `Receipts` are reference-counted in hot RocksDB, but cold storage fixes the count at one. |
| Outcomes and proofs | `OutcomeIds`, `TransactionResultForBlock`, feature-gated `ReceiptProofs` | Outcomes have both a block/shard list and an `(outcome_id, block_hash)` value path. |
| Historical trie | `State`, `StateShardUIdMapping` | `State` is content-addressed by `(ShardUId, trie object hash)` and is the hardest cold workload. |
| Feature-gated | nightly `ChunkProducers` | SPICE columns other than `ReceiptProofs` are not currently cold and need a new review if that changes. |

This list is not the same as "everything an archive needs." Several compact columns have
`GcPolicy::Permanent` and intentionally remain in hot RocksDB, including `BlockHeight`,
`BlockMerkleTree`, `BlockOrdinal`, `EpochInfo`, `EpochStart`, `EpochLightClientBlocks`,
`EpochSyncProof`, and `EpochValidatorInfo`. Keeping them there avoids duplicating the control-plane
indexes ClickHouse would otherwise have to serve.

### Encoding that must remain compatible

- Most values are Borsh-serialized protocol or chain structs.
- A `CryptoHash` key is 32 raw bytes.
- Heights and ordinals use eight-byte little-endian integers.
- `(block_hash, shard_id)` is 32 hash bytes followed by an eight-byte little-endian `ShardId`.
- `(outcome_id, block_hash)` is two concatenated 32-byte hashes.
- `ShardUId` is eight bytes: four-byte little-endian layout version followed by four-byte
  little-endian shard ID.
- `State` is keyed by `ShardUId || trie_node_or_value_hash`.
- `StateChanges` is keyed by `block_hash || trie_key`, with a `ShardUId` suffix for delayed-receipt
  rows that require it.

The ClickHouse writer should preserve raw value bytes wherever the source row exists. Rebuilding a
value from a newer Rust struct and serializing it again is a compatibility risk, even when the
decoded value is logically equivalent.

## Reads that the archive must actually serve

| Read | Current path | Shape | Required ClickHouse path |
| --- | --- | --- | --- |
| Block by height | `BlockHeight[height] -> hash`, then `Block[hash]` | Two point reads; height index is hot/permanent | Hot RocksDB height lookup, then block locator and one fact row. |
| Block/header by hash | `Block[hash]`, `BlockHeader[hash]` | Point lookup | Block locator, then extract block or header from one fact row. |
| All blocks at height | `BlockPerHeight[height]` | Point lookup returning fork sets | Preserve raw exception row unless fork semantics are explicitly redesigned. |
| Next canonical block | `NextBlockHashes[hash]` | Point lookup | Resolve the block locator and read `next_block_hash`. |
| Chunk by hash | `Chunks[chunk_hash]` | Point lookup | Chunk locator, then one shard fact row. |
| Transaction by hash | `Transactions[tx_hash]` | Point lookup | Transaction locator, then decode one indexed transaction from the chunk payload. |
| Receipt by hash | `Receipts[receipt_id]` | Point lookup | Receipt locator, then decode the indexed receipt from its containing shard payload. |
| Receipt origin | `ReceiptToTx[receipt_id]` | Point lookup | Receipt locator, then indexed `receipt_to_tx` entry. |
| Outcome by ID and block | `TransactionResultForBlock[id || block_hash]` | Composite point lookup | Outcome locator ordered by `(id, block_hash)`, then indexed outcome in one shard fact row. |
| All outcomes for an ID | Prefix scan of `TransactionResultForBlock[id]` | Short ordered prefix scan | Contiguous locator range for one outcome ID. |
| Outcomes in a block | `OutcomeIds[block_hash || shard_id]`, then one point read per ID | Block/shard point plus fan-out | Read the outcome arrays directly from each shard fact row. |
| State changes in a block | Prefix scan of `StateChanges[block_hash]` | Sequential block-local scan | Read state-change key/value arrays for the block's shard rows. |
| Receipt proofs for a target shard | Prefix scan of `ReceiptProofs[block_hash || to_shard]` | Short ordered prefix scan | Read the proof arrays in one shard fact row. |
| Trie object | `State[shard_uid || hash]` | Repeated dependent point reads | Direct lookup in `trie_objects`; cache and batch frontier reads where possible. |
| Indexer stream | Consecutive blocks, new chunks, outcomes, receipts, changes | Height range scan | Sequential reads from height-ordered fact tables. |
| Migration/validation | Full column iteration and prefix/range scans | Bulk sequential | Offline ClickHouse scans or the raw exception table; not a latency-sensitive RPC path. |

The important distinction is that a transaction hash lookup and a historical state query are both
"point reads" at the database API, but their costs differ. A transaction lookup needs one locator
and one container. A trie query discovers each next hash only after decoding the previous node, so a
single view call may produce tens or hundreds of dependent cold reads.

## Design

### One compressed history, one small catalogue

The target model stores each large payload once in a height/shard container and stores only the
coordinates needed to find it in a hash-ordered catalogue. It is deliberately not a fully normalized
explorer schema.

```text
                              append final heights
                                      |
                                      v
hot RocksDB ---> archive writer ---> ClickHouse hot NVMe volume
     |                                |             |
     |                                |             +-- archive_locators (all history)
     |                                |
     |                                +-- blocks_by_height
     |                                +-- shard_records_by_height
     |                                +-- raw_cold_rows
     |                                |
     |                                | TTL move + recompress
     |                                v
     |                          ClickHouse cold volume
     |                          (HDD/NVMe, large parts)
     |
view request
     |
     v
archive resolver ---> locator lookup ---> height/shard fact lookup
     |                                      |
     +---------- hydrate bounded local cache+
                         |
                         v
                  existing Store readers
```

This design spends random, mostly incompressible hash bytes once in `archive_locators`. In return,
blocks, chunks, transactions, receipts, and outcomes do not each need their own duplicate payload
table. The catalogue stays on NVMe because its size and latency determine whether cold point reads
remain practical.

### Logical tables

The schemas below are logical contracts. Exact ClickHouse types and settings remain benchmark
inputs, not decisions to copy into production without measurement.

#### `blocks_by_height`

One row per produced canonical block. Skipped heights have no row.

| Column | Type | Purpose |
| --- | --- | --- |
| `block_height` | `UInt64` | Fact-table primary coordinate. |
| `block_timestamp` | `DateTime64(9, 'UTC')` | Hot/cold TTL and operator queries. |
| `block_hash` | `FixedString(32)` | Validation and direct joins after locator resolution. |
| `prev_block_hash` | `FixedString(32)` | Chain validation and previous navigation. |
| `next_block_hash` | `FixedString(32)` | Reconstructs `NextBlockHashes`. |
| `epoch_id` | `FixedString(32)` | Epoch grouping without decoding the block. |
| `block_value` | `String` | Exact raw `DBCol::Block` value bytes. |
| `block_info_value` | `String` | Exact raw `DBCol::BlockInfo` value bytes. |
| `chunk_producer_keys` | `Array(UInt64)` | Feature-gated shard IDs, empty when unavailable. |
| `chunk_producer_values` | `Array(String)` | Exact raw values, position-aligned with the keys. |
| `archive_format_version` | `UInt16` | Version of this table's reconstruction contract. |

`BlockHeader` is reconstructed from `block_value`; storing its full serialized value separately
would preserve today's RocksDB duplication. A byte-parity benchmark must verify that extracting and
serializing the header remains compatible before dropping the duplicate.

#### `shard_records_by_height`

One row per block and `ShardUId`. A row records whether the block contains a new chunk or carries the
previous chunk forward.

| Column | Type | Purpose |
| --- | --- | --- |
| `block_height` | `UInt64` | Fact-table primary coordinate. |
| `block_timestamp` | `DateTime64(9, 'UTC')` | Placement and range filtering. |
| `block_hash` | `FixedString(32)` | Reconstructs block-prefixed keys. |
| `shard_uid_version` | `UInt32` | Dynamic-resharding layout version. |
| `shard_id` | `UInt32` | The second half of `ShardUId`. |
| `record_kind` | `Enum8('new_chunk' = 1, 'carried' = 2)` | Avoids `Nullable` masks for carried rows. |
| `chunk_hash` | `FixedString(32)` | All zeroes for a carried row. |
| `chunk_value` | `String` | Exact raw `DBCol::Chunks` value, empty for carried rows. |
| `chunk_extra_value` | `String` | Exact raw `DBCol::ChunkExtra` value. |
| `chunk_apply_stats_value` | `String` | Exact raw `DBCol::ChunkApplyStats` value. |
| `incoming_receipts_value` | `String` | Exact raw vector stored for this block/shard, empty if absent. |
| `outgoing_receipts_value` | `String` | Exact raw vector, empty for carried rows where unavailable. |
| `state_change_keys` | `Array(String)` | Raw suffixes after `block_hash`, including delayed-receipt shard suffixes. |
| `state_change_values` | `Array(String)` | Exact raw values aligned with `state_change_keys`. |
| `outcome_ids` | `Array(FixedString(32))` | Reconstructs `OutcomeIds`. |
| `outcome_values` | `Array(String)` | Exact `TransactionResultForBlock` values aligned with IDs. |
| `receipt_to_tx_ids` | `Array(FixedString(32))` | Receipt IDs with origin mappings. |
| `receipt_to_tx_values` | `Array(String)` | Exact values aligned with IDs. |
| `receipt_proof_keys` | `Array(String)` | Feature-gated proof key suffixes. |
| `receipt_proof_values` | `Array(String)` | Exact proof values aligned with keys. |
| `archive_format_version` | `UInt16` | Version of this row's reconstruction contract. |

Arrays keep block-local iteration sequential and let ClickHouse compress common structure across
adjacent heights. They also retain exact row values, which lets the middleware reproduce existing
`Store` results without teaching every caller about a new domain model.

#### `archive_locators`

One compact row per hash-addressable object. This is the catalogue that prevents historical scans.

| Column | Type | Purpose |
| --- | --- | --- |
| `object_kind` | `Enum8` | `block`, `chunk`, `transaction`, `receipt`, or `outcome`. |
| `object_hash` | `FixedString(32)` | The requested object ID. |
| `context_hash` | `FixedString(32)` | Block hash for outcomes; all zeroes for unique objects. |
| `block_height` | `UInt64` | Locates the fact-table row. |
| `shard_uid_version` | `UInt32` | Locates a shard row after resharding. |
| `shard_id` | `UInt32` | Locates a shard row. |
| `container_kind` | `Enum8` | Selects block, chunk, receipt list, outcome list, or mapping list. |
| `item_index` | `UInt32` | Position inside the selected container. |
| `byte_offset` | `UInt32` | Optional direct slice offset when the exporter can prove it is stable. |
| `byte_length` | `UInt32` | Optional direct slice length; zero means decode by ordinal. |

`ORDER BY (object_kind, object_hash, context_hash)` makes an object lookup a sparse primary-index
search rather than an O(n) scan. `get_outcomes_by_id` becomes one contiguous range because all block
contexts for an outcome ID are adjacent.

The table stays on NVMe for all history. It should initially use no partition key so the ordering is
global and hash lookups do not fan out across time partitions. The benchmark must verify that
background merges keep the active part count bounded. If the table outgrows the NVMe budget, hash
prefix partitioning and a smaller external exact index are alternatives, not silent degradations.

#### `trie_objects`

One row per cold `DBCol::State` row.

| Column | Type | Purpose |
| --- | --- | --- |
| `shard_uid_version` | `UInt32` | Preserves the state namespace. |
| `shard_id` | `UInt32` | Preserves the state namespace. |
| `object_hash` | `FixedString(32)` | Trie node or external value hash. |
| `object_value` | `String` | Raw trie node or value bytes with no RocksDB refcount suffix. |
| `first_seen_height` | `UInt64` | Migration validation and optional placement analysis. |

`ORDER BY (shard_uid_version, shard_id, object_hash)` serves the exact trie lookup. This table can be
written directly to the cold volume because recent state is already available from hot RocksDB.
Partitioning by a small hash-prefix bucket can cap part size and permit pruning because every query
contains `object_hash`; 64 buckets is a benchmark candidate, not a fixed value.

The first version preserves the existing per-`ShardUId` namespace. A globally deduplicated
`object_hash -> payload` table plus a `(ShardUId, object_hash)` membership table may save payload
bytes across reshardings, but it adds one join and one membership row per object. It should be
adopted only if real data shows a net win.

#### `archive_epoch_data`

One row per epoch with the versioned epoch envelope: epoch ID, epoch info, shard layout, epoch start,
sync height, previous hash, and initial block Merkle tree. This small table makes the archive
self-describing and supports disaster recovery. It is deliberate duplication of compact permanent
RocksDB data and stays on NVMe.

#### `raw_cold_rows`

A compatibility escape hatch for rows not yet reconstructable from the fact tables.

| Column | Type | Purpose |
| --- | --- | --- |
| `column_id` | `UInt8` | Stable `DBCol` identifier. |
| `row_key` | `String` | Exact RocksDB key bytes. |
| `row_value` | `String` | Exact value bytes with cold refcount semantics. |
| `first_seen_height` | `UInt64` | Placement, validation, and migration ownership. |

Initial candidates include `BlockPerHeight`, `StateHeaders`, `StateChangesForSplitStates`, and any
feature-gated row not covered by the typed envelopes. The target is not to mirror every cold row
here forever; that would retain all current duplication and reduce ClickHouse to a remote KV table.

#### `archive_commits`

One row per deterministic ingest batch with network/genesis identity, start and end heights, shard
layout version, row counts, uncompressed byte counts, content digests, writer version, and status.
Readers only expose the highest contiguous committed height. This manifest is the visibility fence
because ClickHouse does not provide an atomic transaction across all archive tables.

### Sorting and partitioning

| Table | `PARTITION BY` candidate | `ORDER BY` | Placement |
| --- | --- | --- | --- |
| `blocks_by_height` | `intDiv(block_height, 1000000)` | `(block_height, block_hash)` | Recent parts NVMe, old parts cold. |
| `shard_records_by_height` | `intDiv(block_height, 1000000)` | `(block_height, shard_uid_version, shard_id)` | Recent parts NVMe, old parts cold. |
| `archive_locators` | `tuple()` | `(object_kind, object_hash, context_hash)` | NVMe for all history. |
| `trie_objects` | hash-prefix bucket candidate | `(shard_uid_version, shard_id, object_hash)` | Cold volume directly. |
| `archive_epoch_data` | `tuple()` | `(epoch_id)` | NVMe. |
| `raw_cold_rows` | `(column_id, intDiv(first_seen_height, 1000000))` candidate | `(column_id, row_key)` | Recent NVMe, old cold. |
| `archive_commits` | `tuple()` | `(start_height, end_height)` | NVMe. |

One-million-height partitions are an initial compromise: coarse enough to avoid a partition
explosion, but small enough to validate, detach, or rebuild a bounded range. Height is more stable
than wall-clock month boundaries for protocol data. The stored timestamp still drives placement
TTL, and height/timestamp correlation means completed parts age together.

ClickHouse primary indexes are sparse and index granules rather than individual rows. The locator
benchmark should test `index_granularity` values such as 256, 512, and 1024. Fact tables should use
adaptive byte granularity so one lookup does not decompress a granule containing many large Borsh
blobs. Smaller granules consume more marks and memory, so the choice must be based on total index
bytes as well as latency. See the [MergeTree documentation](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree)
and [sparse primary-index guide](https://clickhouse.com/docs/optimize/sparse-primary-indexes).

### Compression choices

| Data | Initial codec candidates | Rationale |
| --- | --- | --- |
| Heights | `Delta(8), ZSTD(3)` | Height-ordered rows have small monotonic deltas. |
| Timestamps | `DoubleDelta, ZSTD(3)` | Block times are nearly monotonic with a mostly stable interval. |
| Shard IDs and layout versions | `T64, ZSTD(3)` or plain `ZSTD(3)` | Small integer domains; actual gain depends on row ordering. |
| Enums and booleans | `Enum8` / `UInt8`, then `ZSTD(1)` | Avoid repeated strings and `Nullable` masks. |
| Hashes | `FixedString(32)` with `NONE` or `ZSTD(1)` | Cryptographic hashes are intentionally incompressible; compression may only add CPU. Test both. |
| Raw Borsh payloads | `ZSTD(3)`, `ZSTD(6)`, and `ZSTD(9)` | Cold ingest can spend more CPU, but gains above level 3 must pay for themselves in bytes. |
| State-change and outcome arrays | Element types above plus `ZSTD(6)` or `ZSTD(9)` | Adjacent block/shard rows may share structural byte patterns. |
| Account IDs in any optional query table | `String, ZSTD` | Global cardinality is high; `LowCardinality(String)` is not a safe default. |

ClickHouse's own compression guidance recommends ZSTD as the general compression choice, Delta for
monotonic integer sequences, and notes that levels above 3 often add CPU for little gain. This
project values bytes more than ingest CPU, so levels 6 and 9 are valid experiments, but the design
does not assume they win. See the [official compression guide](https://clickhouse.com/docs/data-compression/compression-in-clickhouse)
and [codec reference](https://clickhouse.com/docs/sql-reference/statements/create/table#column_compression_codecs).

An attractive lifecycle is to ingest recent parts with `ZSTD(3)`, then use a TTL recompression rule
to apply the winning cold codec before moving the part to the cold volume. This keeps the tailer
ahead of finality without giving up cold compression.

### Hot and cold ClickHouse volumes

Use two explicit storage policies:

- `archive_tiered` contains a hot NVMe volume followed by a cold volume backed by HDDs or capacity
  NVMes. The height-ordered facts and raw exceptions use it.
- `archive_index` contains NVMe only. Locators, manifests, and epoch metadata use it.
- `archive_state` may write trie objects directly to the cold volume because hot RocksDB already
  serves recent trie nodes.

The hot retention window should be set by measured query age, not by convention. Candidates are 7,
30, and 90 days. ClickHouse can move parts with `TTL ... TO VOLUME` and can also move parts when a
volume crosses `move_factor`. The TTL is the normal lifecycle; `move_factor` is an emergency space
pressure valve. See [external disks and storage policies](https://clickhouse.com/docs/operations/storing-data)
and [TTL management](https://clickhouse.com/docs/guides/developer/ttl).

Multiple disks in one ClickHouse volume distribute parts but do not make them redundant. If this is
the sole archive, use an explicitly reviewed durability design such as replicated MergeTree tables
across failure domains or redundant storage below ClickHouse. A single replica is acceptable only
when the retained RocksDB archive or another immutable copy provides a tolerable rebuild path.

### No secondary indexes by default

The baseline has no projections and no data-skipping indexes. A projection is effectively another
hidden ordering of the data and therefore duplicates storage. Bloom filters also consume bytes and
still admit false positives. The hash-ordered locator table gives exact coordinates with a cost that
can be measured directly.

Add a secondary index only when a recorded production query cannot be served by:

- the fact-table ordering;
- the locator ordering;
- a bounded block/shard range; or
- a small permanent RocksDB index.

This follows the storage-first constraint and avoids speculative account/activity indexes. See the
[projection documentation](https://clickhouse.com/docs/data-modeling/projections) and
[data-skipping index documentation](https://clickhouse.com/docs/optimize/skipping-indexes).

### Writer and consistency model

The writer follows the existing cold loop rather than tailing unfinalized blocks:

1. Read the highest contiguous committed height and the RocksDB final head.
2. Build deterministic batches of final heights. Reuse the key discovery and resharding rules in
   `update_cold_db`, including genesis state and `StateShardUIdMapping` behavior.
3. Insert fact rows, trie objects, exceptions, and locators in large sorted blocks.
4. Validate row counts and content digests for every table in the batch.
5. Append the committed manifest row.
6. Advance the local ClickHouse archive head in hot RocksDB only after the manifest is durable.

ClickHouse tables do not enforce primary-key uniqueness. The writer must therefore make a retry
either absent or byte-identical. Use deterministic batch IDs and replicated-insert deduplication
where available, but do not rely on a bounded deduplication window as the correctness mechanism.
When a locator query returns duplicate rows, the reader verifies that their coordinates are
identical and reports corruption otherwise.

No normal writer path mutates archived data. A finality violation, protocol bug, or operator repair
uses an explicit offline range-rebuild procedure with the affected manifest range unavailable to
readers until recommitted.

### Embedded nearcore reader

Implementing a remote SQL call directly inside every `Database::get_raw_bytes` is the smallest code
diff and the wrong runtime boundary. The trait is synchronous, trie reads are dependent, and actor
threads would block on network and decompression.

The recommended boundary is a typed `ArchiveReader` used by view and indexer request handling:

1. Try the existing hot `Store` first.
2. On an archive miss, resolve the object in `archive_locators` or by block height.
3. Fetch the minimum fact rows, preferably an aligned height batch for sequential consumers.
4. Validate the archive format version and Borsh payloads.
5. Materialize exact `DBCol` rows into a bounded local archive cache on NVMe.
6. Retry the existing `ChainStoreAdapter` or trie read against the composed store.

This is the same broad pattern as the current cloud archival reader, which downloads typed batches
and writes ordinary rows into a local store. The local cache is disposable and is not the archive's
source of truth. RocksDB is a reasonable first cache engine because it already satisfies the
`Database` contract; a simpler cache can replace it if measurements justify the work.

Q: Why materialize rows instead of returning new ClickHouse-specific domain objects?

A: Existing view, indexer, trie, and debug paths already compose typed behavior over `Store`.
Materialization contains the storage-specific code and reduces the chance that a rarely used
archival path silently changes semantics.

Q: Does this avoid every blocking read?

A: No. A historical trie traversal can miss after each decoded node. The reader needs a request
deadline, an object cache, and an `IN` query for every known frontier. If that remains too slow, cold
`DBCol::State` stays in a key/value store; the transactional history can still move to ClickHouse.

### What remains in RocksDB

The hot RocksDB remains authoritative for:

- `DbVersion`, `BlockMisc`, heads, tails, archive progress, and database metadata.
- `BlockHeight`, `BlockOrdinal`, `BlockMerkleTree`, epoch information, validator information, and
  other compact permanent indexes.
- Current and recent `State`, `TrieChanges`, flat state, flat-state deltas, and state snapshots.
- State sync downloads, catchup work, partial chunks, witness/debug caches, networking data,
  compiled-contract caches, and all node-local operational columns.
- Unfinalized blocks, forks inside the hot retention horizon, and everything consensus can read or
  write on its critical path.
- A bounded read-through archive cache, if that integration proves simpler than changing the
  synchronous database interface.

The target permits historical `State` in ClickHouse, but it is a separate go/no-go gate. If a cold
historical view call cannot meet the agreed latency and CPU budget, retain the full cold `State`
column in RocksDB or another exact-key store. This hybrid still removes block, chunk, transaction,
receipt, outcome, and state-change history from the 110 TiB RocksDB archive.

## Where the bytes are duplicated today

The existing column contract intentionally pays storage for independent lookup paths:

- A block contains its header, while `BlockHeader` stores the header again by block hash.
- A full chunk contains signed transactions, while `Transactions` stores each transaction again by
  transaction hash.
- Receipts occur in chunk and receipt-proof structures, while `Receipts` stores independently
  addressable copies.
- `OutcomeIds` stores per-block/shard ID lists, while each outcome ID is also part of the
  `TransactionResultForBlock` key.
- Block, chunk, outcome, and receipt indexes repeat hashes to make RocksDB point lookups cheap.
- Every cold reference-counted `State`, transaction, and receipt value carries reference-count
  representation that is unnecessary in an immutable ClickHouse row.

The proposed locators remove large duplicated payloads but retain the minimum repeated coordinates
needed for exact lookup. This is a hypothesis until a byte census shows the share held by each
column. If trie objects dominate nearly all 110 TiB, eliminating transaction duplication may be
correct but economically unimportant.

## Alternatives considered

### Keep split RocksDB and tune compression

This preserves all interfaces and gives the best point-read behavior. It was not selected as the
target because RocksDB keeps payload duplication and per-key LSM overhead, and its column-family
model cannot exploit height/shard columnar compression. It remains the baseline every ClickHouse
result must beat.

### Mirror every cold `DBCol` row into one ClickHouse KV table

Schema: `(column_id, key, value) ORDER BY (column_id, key)`. This is the fastest path to functional
parity and is valuable as a migration prototype. It was rejected as the target because it retains
all duplicated values, mixes unrelated key shapes, and makes ClickHouse a slower remote KV store.
Use it to validate the reader, not as proof that the final design is storage-efficient.

### Fully normalize transactions, actions, receipts, and state changes

This gives excellent analytical filtering and account history queries. It was rejected as the
baseline because strings, hashes, action metadata, and join keys are repeated across many tables.
It also couples the archive schema to every protocol enum version. Add derived query tables after
the compressed archive is proven and only for measured workloads.

### Store versioned block/shard blobs only, with no locators

This is close to the cloud archive and likely compresses well. It was rejected because a transaction,
receipt, chunk, or outcome hash would require an O(n) height scan or an external catalogue. The
locator table is the minimum extra structure that makes blob grouping operationally useful.

### Put all locators in RocksDB

RocksDB is a better exact-key engine and would simplify point lookup latency. It was not selected as
the initial target because the stated goal is for ClickHouse to manage the archive and its hot/cold
footprint, and a large second permanent RocksDB index complicates capacity planning. It remains a
credible fallback if the ClickHouse sparse index or NVMe locator size fails the benchmark.

### Use the cloud archival blobs directly

The current cloud format already groups 32 heights with ZSTD level 3 and versioned block/shard/epoch
envelopes. It is a strong source model and recovery format. It was not selected as the query store
because object retrieval needs a separate exact catalogue and does not provide ClickHouse range
queries, managed disk tiers, or column-level compression experiments. The two designs should share
exporter and reconstruction code where possible.

## Cross-cutting concerns

### Correctness and protocol evolution

- Every archive belongs to one chain identity: chain ID, genesis hash, and genesis height are stored
  and checked by the writer and reader.
- Only final canonical heights are committed. The contiguous committed head is the visibility
  boundary.
- `ShardUId`, not only `ShardId`, is stored anywhere state namespace matters. Dynamic resharding
  otherwise creates ambiguous trie keys.
- Raw Borsh bytes are retained for database values. Typed decoded columns are routing metadata, not
  a new protocol serialization.
- Each envelope and manifest carries a format version. Readers reject unsupported versions rather
  than guessing.
- A new cold `DBCol` or protocol feature must fail an exhaustive coverage test until it is mapped to
  a fact field, locator, exception row, or explicit hot-only decision.

### Security

On-chain data is public, but the archive service still exposes operational attack surface.

- nearcore gets a read-only ClickHouse identity. The writer gets insert access only to archive and
  staging tables.
- Use TLS and private network policy between nearcore and ClickHouse.
- Use prepared, fixed-shape queries. Raw RPC values never become SQL fragments.
- Apply per-user query time, memory, result-size, and concurrency limits so an archive RPC cannot
  evict ingestion or overload HDDs.
- Encrypt disks or volumes according to the node operator's threat model, and keep credentials out
  of `config.json` backups and logs.

### Privacy

The baseline stores only public on-chain data and operational manifests. It must not add client IPs,
RPC authentication details, or request histories to the archive tables. Query logs are operational
data with a separate retention and access policy.

### Observability

Expose at least:

- RocksDB final head, ClickHouse committed head, and lag in heights and seconds.
- Rows and uncompressed/compressed bytes written per logical family.
- Part count, merge backlog, mutation/TTL backlog, and free bytes per disk and volume.
- Locator and fact query latency by object kind, cache state, and storage tier.
- Rows, marks, and bytes read per lookup. A point query reading a large fraction of a partition is an
  alertable regression.
- Archive cache hit ratio, size, evictions, corrupt entries, and hydration latency.
- Borsh decode failures, duplicate locator disagreements, missing manifest ranges, and digest
  mismatches.
- Historical trie nodes fetched per view call and the number of dependent ClickHouse round trips.

### Operability

- Pin and test one ClickHouse release before producing the archive. Codec and query-plan behavior
  are part of the benchmark result.
- Keep schema, storage policies, users, quotas, and migrations in reviewed configuration.
- Back up archive manifests, table metadata, and any data not reproducible within the agreed RTO.
- Document disk replacement, replica recovery, range rebuild, stalled merges, and rollback to cold
  RocksDB before cutover.
- Capacity planning includes temporary merge space. Compressed steady-state size alone is not a
  safe disk requirement.

### Required reviews

- nearcore storage owners: `DBCol` coverage, byte compatibility, and split-store behavior.
- chain/runtime owners: state, receipt, outcome, and resharding semantics.
- RPC/indexer owners: required query set and acceptable cold latency.
- SRE/storage owners: ClickHouse topology, durability, capacity, backup, and on-call procedures.
- Security owners: credentials, network policy, resource limits, and query-log handling.

## Risks and incompatibilities

| Risk | Consequence | Mitigation or decision gate |
| --- | --- | --- |
| Sparse rather than row-level primary index | A point lookup reads a granule from every matching active part. | NVMe locator table, small locator granularity, bounded part count, and `EXPLAIN indexes = 1` tests. |
| Synchronous `Database` API | Network I/O can block actor threads and amplify failures. | Typed async resolver plus local materialization; no ClickHouse calls on consensus paths. |
| Dependent trie traversal | A historical view call can require many sequential cold round trips. | Cache, frontier batching, request deadlines, and a separate state go/no-go gate. |
| No cross-table transaction | A crash can leave facts without locators or vice versa. | Deterministic batches, digests, commit manifest last, and contiguous-head visibility. |
| No uniqueness enforcement | Retry bugs can create duplicate locator rows. | Idempotent batches, duplicate equality checks, and repair tooling. |
| Borsh/protocol evolution | Re-serialization may not reproduce stored bytes. | Preserve raw values and version reconstruction logic. |
| Dynamic resharding | `ShardId` alone can resolve the wrong state namespace. | Store full `ShardUId` components and validate mapping transitions. |
| Fork data | A height-only canonical model can discard `BlockPerHeight` semantics. | Preserve raw fork maps initially and define a separate fork-retention decision. |
| Feature drift | SPICE or new columns can silently escape archival coverage. | Exhaustive `DBCol` coverage test and fail-closed writer startup. |
| Random data compression | Hashes, signatures, and encrypted-looking WASM/data bytes may not compress. | Byte census and codec matrix before capacity claims. |
| Multiple disks without redundancy | One disk failure can lose a subset of parts. | Replication or redundant lower storage; never call JBOD a backup. |
| Merge amplification | High compression can still require large temporary free space and I/O. | Large sorted inserts, part-count limits, merge headroom, and production-like disk tests. |
| Cloud archive format is not stable | Sharing code too tightly can inherit an unfinished contract. | Share builders and concepts, but version the ClickHouse format independently. |

## Migration from an existing archive node

### Phase 0: measure before designing capacity

Run a read-only census of the existing cold RocksDB:

- Physical and logical bytes, row count, key bytes, value bytes, and compression estimate per
  `DBCol`.
- Value-size histograms and duplicate-payload estimates for blocks/headers, chunks/transactions,
  receipts, outcomes, and trie objects.
- State-object duplication across `ShardUId`, especially before and after resharding.
- Rows and bytes created per million heights and per protocol era.
- Production RPC and indexer query frequency by lookup kind and age.

This phase decides whether the proposed deduplication attacks the actual size. Do not procure the
final ClickHouse cluster from the 110 TiB headline alone.

### Phase 1: prove byte parity with a raw mirror

Export a bounded representative range to a simple raw ClickHouse KV table and implement the
read-through resolver against it. Differentially compare every supported `Store` read with the cold
RocksDB result. This deliberately inefficient model isolates integration correctness from schema
optimization.

### Phase 2: bulk-load the target schema

1. Create a consistent RocksDB checkpoint or keep the source archive read-only for the exported
   range.
2. Record a final boundary `H0` and chain identity.
3. Export canonical heights from genesis through `H0` in deterministic ranges. Include protocol-era
   and resharding metadata in each manifest.
4. Sort inserts by each table's `ORDER BY` key and write large blocks to avoid small parts.
5. Validate counts, uncompressed bytes, digests, locator coverage, and random byte-for-byte reads
   before committing each range.
6. Resume from the highest contiguous committed range after interruption.

The exporter should reuse the current cold loop's key discovery rather than scanning each column and
trying to infer height later. `StateChanges` is already copied with one block-hash prefix scan, and
`State` is discovered from per-shard `TrieChanges`; those are efficient ingest boundaries.

### Phase 3: tail and shadow-read

- Start the final-height writer at `H0 + 1` while the RocksDB cold loop remains authoritative.
- Send a sampled percentage of eligible archive reads to both stores and compare results and errors.
- Run indexer range streams and historical RPC queries against ClickHouse without serving their
  results to users.
- Increase shadow coverage only while lag, error rate, and query budgets remain inside gates.

### Phase 4: cut over transactional history

Route blocks, chunks, transactions, receipts, outcomes, and state changes through the archive
resolver. Keep the old cold RocksDB mounted read-only as a fallback. A feature flag must restore the
old split store without data conversion or a ClickHouse mutation.

### Phase 5: decide historical state separately

Run the trie benchmark and historical view-call differential tests. Move `DBCol::State` only if the
result meets the operational latency and CPU budget. Otherwise keep state in cold RocksDB and record
that hybrid as the accepted architecture rather than hiding it as unfinished migration.

### Phase 6: retire the old cold store

Retirement requires:

- An agreed shadow period with no unexplained mismatches.
- A tested ClickHouse restore or rebuild procedure inside the RTO.
- Verified durability across the intended disk-failure scenario.
- Capacity alarms and runbooks in production.
- An explicit operator action. No automatic deletion belongs in the migration tool.

## Benchmarks required before commitment

### Dataset

Use several disjoint real-history slices, not only the newest contiguous range:

- Early mainnet history with older serialization versions.
- A high-transaction period.
- A contract/state-heavy period.
- At least one static and one dynamic resharding boundary.
- The current protocol era, including enabled feature-gated columns.

The combined sample should be large enough to form production-sized parts and stress HDD seeks. A
1-5 TiB source sample is a reasonable starting candidate, then validate the winning model on a much
larger range before extrapolating to 110 TiB.

### Storage matrix

Compare at least:

1. Existing cold RocksDB physical and logical bytes.
2. One raw ClickHouse KV table.
3. Height/shard facts plus the locator catalogue.
4. The same model with duplicate block headers, transactions, and receipts retained.
5. Per-`ShardUId` trie objects versus global payload deduplication plus membership.
6. ZSTD levels 1, 3, 6, 9, and 12 for large payloads.
7. Delta/DoubleDelta/T64 combinations for typed numeric columns.
8. One and two durable copies, including filesystem or replica overhead.

Report compressed bytes by table and column from `system.columns`/`system.parts`, not only total disk
usage. Also report peak temporary bytes during merges and TTL recompression.

### Query matrix

Measure cold-cache and warm-cache p50, p95, p99, bytes read, marks read, CPU time, and physical disk
reads for:

- Block by hash and by height.
- Header by hash.
- Chunk by hash.
- Transaction by hash.
- Receipt by hash and receipt-to-transaction origin.
- Outcome by `(outcome_id, block_hash)` and all contexts for one outcome ID.
- All outcomes and state changes for one block.
- 1,000- and 10,000-height indexer streams.
- Historical account/view calls at several state ages and trie depths.
- Concurrent mixes at 1, 16, 64, and the expected production request concurrency.
- Misses for random hashes, because a negative lookup must also remain bounded.

Run the same queries on NVMe fact data and HDD fact data. A fast locator followed by an unbounded HDD
read is not a successful point lookup.

### Ingestion and operations matrix

- Sustained final-height ingestion at 2x the observed chain byte rate, plus catch-up bursts.
- Bulk migration throughput and estimated wall time for the full source.
- CPU and I/O cost of ZSTD recompression and TTL moves.
- Part count and merge backlog under the real batch cadence.
- Writer crash after each table insert and before/after the manifest commit.
- Duplicate batch retry, corrupt row, missing locator, missing fact, and unsupported format version.
- One cold disk loss, one replica loss, restore, and range rebuild.
- nearcore behavior when ClickHouse is slow, unavailable, or returns a partial result.

### Correctness suite

- Byte-for-byte comparison for every covered `DBCol` point read.
- Ordered key/value comparison for required prefix/range iterators.
- Differential JSON-RPC and indexer output over selected historical ranges.
- Full locator coverage: every hash-addressable payload has the required locator, and every locator
  resolves to a matching payload.
- Trie traversal from sampled historical roots, including contract code/value reads and missing
  nodes.
- Resharding boundary validation for state mappings and child shard state.
- Chain continuity, block/next hash consistency, chunk membership, outcome proofs, and epoch layout.

### Suggested decision gates

Exact thresholds belong to the operator workload, but the go/no-go review should require all of the
following categories:

- A material single-copy byte reduction after counting locators, parts, marks, and required
  exceptions.
- Ingestion comfortably ahead of finality while cold recompression and merges run.
- Point queries that read a bounded number of parts/granules and meet the agreed cold RPC SLO.
- Indexer streams that do not starve locator reads.
- Zero unexplained byte or RPC mismatches.
- A tested failure and rollback path.

Do not approve the design based only on a compression ratio from `clickhouse-compressor`; the
locator, part layout, query path, and durability copies are part of the storage cost.

## Implementation sequence if the benchmarks pass

| Increment | nearcore work | ClickHouse work | Feedback signal |
| --- | --- | --- | --- |
| 1. Census | Read-only RocksDB size and workload tooling | None | Real byte distribution and query mix. |
| 2. Raw parity prototype | Typed resolver and disposable local cache | Raw KV staging table | Byte-level differential reads. |
| 3. Fact exporter | Shared block/shard/epoch builders and locators | Target staging tables and manifests | Compression and locator coverage. |
| 4. Shadow tail | Final-height writer and shadow comparator | Tiered policy, quotas, dashboards | Lag, latency, and mismatch rate. |
| 5. Transactional cutover | Feature-gated view/indexer routing | Production facts and locators | Production cold SLO and rollback exercise. |
| 6. State experiment | Batched trie loader and cache | `trie_objects` | Historical view-call gate. |

Each increment leaves a working system and answers one expensive uncertainty before the next one.

## Open questions

- UNCLEAR (storage owners): What percentage of the reported 110 TiB is `State`, and how much is
  block/chunk/transaction/receipt duplication?
- UNCLEAR (RPC/indexer owners): Which non-nearcore queries are required, especially account history,
  action filtering, and receipt graph traversal?
- UNCLEAR (SRE): Is ClickHouse the only durable archive, or can a single copy be rebuilt from a
  retained cloud/RocksDB source within the required RTO?
- UNCLEAR (runtime/storage owners): Can every persisted receipt be located without retaining a
  duplicate `Receipts` payload table across all protocol eras?
- UNCLEAR (storage owners): Must `BlockPerHeight` retain all observed fork hashes forever, or is
  canonical history sufficient for this alternative archive?
- UNCLEAR (cloud archival owners): Which block/shard/epoch builders are intended to become a stable
  shared archive contract?
- UNCLEAR (performance owners): What are the cold RPC latency SLO, maximum historical view-call
  latency, and expected concurrency?
- UNCLEAR (capacity owners): What NVMe budget is available for the all-history locator catalogue?
