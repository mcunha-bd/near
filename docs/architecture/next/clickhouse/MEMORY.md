# NEAR ClickHouse archive implementation handoff

This file is the implementation handoff for a future Codex session running on another machine. It
records what exists in the working tree, what was verified, what remains provisional, and how the
migration from an existing archive node must proceed. Read [DESIGNDOC.md](DESIGNDOC.md) for the
architecture and [README.md](README.md) for the current provisioning instructions.

## Resume here

1. Read the repository `AGENTS.md` before changing code. In particular, do not run the protocol
   schema check or update OpenAPI unless the user explicitly asks. Run `cargo fmt` after Rust edits,
   and run the repository-wide clippy command only immediately before a requested commit.
2. Start from implementation commit `4a72648c173aa333b3f5987e5ff28e42a38502d1`
   (`feat(clickhouse): resume background cold-store bootstrap`). Inspect the working tree before
   editing because the next machine may have additional user changes.
3. Do not assume Cargo caches, a ClickHouse binary, or a running ClickHouse server exist on the
   next machine.
4. Preserve unrelated user-owned changes and keep the ClickHouse implementation changes scoped to
   the files listed below unless the task needs a broader design change.
5. Keep the phase boundary explicit: the working code is a raw key/value parity backend. The
   normalized block, shard, locator, trie, epoch, and commit tables in the design document are not
   implemented yet.

## Current implementation status

The current increment gives nearcore a lossless ClickHouse cold backend while retaining the exact
RocksDB keys, values, Borsh encodings, and existing `DBCol` lookup behavior. It is intended to prove
integration correctness before committing to the more normalized and storage-efficient target
model.

### Implemented in code

The following is implemented in `4a72648c1`:

- `ClickHouseArchiveConfig::bootstrap_from_local_cold_store`, defaulting to `true`. This makes a
  newly recreated disposable ClickHouse table bootstrap automatically when neard restarts.
- Local cold RocksDB and ClickHouse write independently. The normal local cold actor starts first
  and keeps the client usable while ClickHouse bootstraps in the background.
- The ClickHouse actor has two serialized states: bootstrap, then finalized-height tail. It cannot
  tail-write while its bootstrap is in progress.
- A bootstrap captures local `H0` once, checkpoints Borsh progress in the ClickHouse raw table after
  every bounded batch, and writes ClickHouse `HEAD`/`COLD_HEAD` only after all cold columns finish.
- Bootstrap progress includes `H0`, completed cold columns, the last written key, and a sequence
  number used to make the newest `ReplacingMergeTree` checkpoint row win deterministically.
- Restart retry resumes after the stored key in the unfinished column. It validates that the local
  cold store still covers the recorded `H0` before doing so.
- The raw-copy helper now accepts/resumes a `ColdStoreCopyProgress` cursor and checkpoints after
  every source batch; a focused unit test simulates an interruption within `DBCol::Block`.
- The ClickHouse provisioning README describes dual-write, background bootstrap, restart resume,
  and resetting a disposable target by recreating its table.

The implementation commit touches:

- `chain/client/src/archive/cold_store_actor.rs`
- `core/store/src/archive/clickhouse/config.rs`
- `core/store/src/archive/cold_storage.rs`
- `core/store/src/db/clickhouse.rs`
- `core/store/src/db/mod.rs`
- `docs/architecture/next/clickhouse/{MEMORY,README}.md`

### ClickHouse database backend

[core/store/src/db/clickhouse.rs](../../../../core/store/src/db/clickhouse.rs) contains
`ClickHouseDB`, an implementation of near-store's synchronous `Database` trait.

It currently covers:

- HTTP access through `reqwest::blocking::Client`, including connect and request timeouts.
- Optional basic authentication with the password read from a file relative to the neard home.
- Startup validation against `system.columns` so an absent or incompatible raw table fails fast.
- RowBinary inserts that preserve binary keys and values without JSON or hexadecimal value storage.
- Configurable request splitting by approximate bytes and row count.
- Bootstrap-progress writes with a monotonic version above the captured bootstrap height, so the
  `ReplacingMergeTree` row for the checkpoint is unambiguous.
- Point reads by `(chain_id, column_name, row_key)`.
- Ordered full-column, prefix, and bounded-range iteration using `argMax` to resolve replacement
  rows without `FINAL`.
- Reference-count stripping on normal iteration and raw-value preservation on raw iteration.
- Tombstone rows for single-key deletes.
- A latched write error because `Database::write` cannot return `Result`. The archive actor checks
  the latch before advancing its remote head.
- Read failures panic after logging rather than becoming false `None` or empty iterator results.
  This protects correctness but needs an explicit production retry and availability decision.
- Unit tests for RowBinary string and varint round trips.

Bulk deletes, local checkpoints, and SST ingestion are deliberately unsupported. A direct caller
must still wrap `ClickHouseDB` in `ColdDB`; that wrapper turns cold reference counts into the
expected single-reference representation and filters invalid cold operations.

### Configuration and startup wiring

[../../../../core/store/src/archive/clickhouse/config.rs](../../../../core/store/src/archive/clickhouse/config.rs)
defines `ClickHouseArchiveConfig` with endpoint, database, table, username, password file, separate
read/write switches, `bootstrap_from_local_cold_store` (default `true`), timeouts, and insert
limits. Identifier validation prevents database or table names from being interpolated as arbitrary
SQL.

`nearcore/src/config.rs` exposes it as `clickhouse_archival`. `nearcore/src/config_validate.rs`
currently enforces:

- At least one of reads or writes is enabled.
- ClickHouse archival is only configured on an archive node.
- `save_trie_changes` is explicitly `true` when ClickHouse writes are enabled.
- Insert limits are non-zero and SQL identifiers are restricted.

`core/store/src/node_storage/` can attach ClickHouse after RocksDB opens. When `read_enabled` is
true, the split database uses hot RocksDB first and ClickHouse for cold misses. When it is false,
local RocksDB cold storage remains the read backend. A local-only split store is also exposed so a
running migration never reads its source data back from ClickHouse.

`nearcore/src/lib.rs` opens and validates ClickHouse, attaches it to `NodeStorage`, starts an
independent archive actor for writes, and exposes `clickhouse_archive_loop_handle`. Local cold
RocksDB remains the authoritative writer while the ClickHouse actor runs independently.

### Local ClickHouse is the performance baseline

The endpoint supports a remote ClickHouse instance so operators can use a disposable target without
placing it on the node. Do not favor that topology for compression, ingestion-throughput, or lookup
latency evaluation: network transport, TLS, and remote storage variability obscure the database
result. Run the primary corpus and codec benchmarks against a local ClickHouse instance on the same
host (ideally with explicitly mounted fast and cold volumes). Use a remote instance only when the
goal is testing the networked deployment, failure handling, credentials, or operational topology,
and report its network cost separately from ClickHouse storage performance.

### Finalized-height writer and resumability

`chain/client/src/archive/cold_store_actor.rs` reuses the existing cold-copy logic for the
ClickHouse target. Before writing a height it sets that height on `ClickHouseDB`; after each logical
write it checks the latched remote error before writing or accepting the remote `HEAD`.

The remote progress marker is stored in ClickHouse as the normal `DBCol::BlockMisc` `HEAD` row. It
is independent from the local cold-store head, so restarts resume each backend from its own
position. Retried RowBinary batches are safe at the logical model because the table uses
`ReplacingMergeTree(archive_height)` and reads select the latest version.

There are two source paths:

- Legacy archive RocksDB without a split cold store: replay canonical finalized heights from the
  archival RocksDB.
- Existing split archive: when the ClickHouse table is empty, a background bootstrap actor records
  the local cold `HEAD` as `H0`, then bulk-streams local cold rows in the background while the local
  cold writer continues running. After every successful bounded insert batch, it persists a Borsh
  `ClickHouseBootstrapProgress` row in `DBCol::BlockMisc`: the captured `H0`, completed-column
  count, last copied key, and monotonic checkpoint version. After a process restart, it resumes at
  the next key in that same column for the original `H0`. It refuses to resume if the local cold
  head no longer covers that recorded head or the checkpoint is structurally invalid. Only after
  every column is copied does it write `HEAD` and `COLD_HEAD` as `H0`, then start the finalized-height
  tail through a local hot+cold split source. Rows after `H0` may be copied early but are replayed
  by the tail before becoming visible.

`core/store/src/archive/cold_storage.rs::copy_all_cold_data` implements the resumable bulk copy. It
uses ordered `iter_range` from the checkpoint key, skips that already committed key, checkpoints
after every bounded transaction, and deliberately writes the captured `HEAD` and `COLD_HEAD` last.
An interrupted import therefore has no committed target head and resumes without repeating its
whole active column.
`ClickHouseDB` stops accepting writes after the first latched error, which prevents a partially
failed bulk import from writing the markers later in the same attempt.

### ClickHouse provisioning artifacts

- [001_archive.sql](001_archive.sql) creates the raw parity table, an ingestion-progress view, and
  a latest-live-row view. It uses a primary key ordered by chain, NEAR column, and raw key, high ZSTD
  compression for values, and replacement by archive height.
- [storage_configuration.xml](storage_configuration.xml) is an example policy with a fast NVMe
  volume and a multi-disk cold volume. Paths, disk count, replication, and durability are operator
  decisions, not portable defaults.
- [README.md](README.md) documents provisioning, least-privilege access, configuration, dual-write,
  validation, and read cutover.

The raw table is the implemented Phase 1 model. The normalized tables described in
[DESIGNDOC.md](DESIGNDOC.md) remain a proposal pending corpus measurements and benchmarks.

## Verification completed on the original machine

The following commands passed after the latest Rust changes:

```text
cargo fmt --check
git diff --check
```

The new focused resume test was added at
`archive::cold_storage::test::test_copy_all_cold_data_resumes_from_last_key`. It simulates an
interruption after a bounded `DBCol::Block` batch and verifies that the resumed copy contains every
source row. It was attempted with the repository-required command:

```text
cargo test --package near-store --features test_features -- archive::cold_storage::test::test_copy_all_cold_data_resumes_from_last_key --exact --show-output
```

The process ended while compiling its large dependency closure and did not return a test result.
Treat this as unverified, not passing.

The production compile command was also attempted:

```text
cargo check -p near-store -p near-client -p nearcore --message-format short
```

It did not reach nearcore source compilation. `clang-sys` failed while building the RocksDB
dependency because this host has neither `llvm-config` nor a discoverable `libclang.so`. Set
`LLVM_CONFIG_PATH`/`LIBCLANG_PATH` or install an appropriate LLVM/Clang package before retrying.

No ClickHouse server or `clickhouse-local` binary was available on the original machine. Therefore:

- The SQL and storage-policy XML have not been applied to a real server.
- No HTTP or RowBinary interoperability test has exercised the running server.
- No live RocksDB-to-ClickHouse migration has been run.
- No byte-parity, compression, latency, failure-recovery, or cutover benchmark has been run.

The next session must not describe this implementation as production-ready until those checks pass.

## Pending implementation work

### Immediate correctness and integration work

1. Add a real ClickHouse integration-test fixture pinned to an explicit server version. Apply
   `001_archive.sql`, exercise every `Database` read/write method, and test binary keys and values,
   reference-counted columns, tombstones, retries, and multi-request partial failures.
2. Run the new `copy_all_cold_data` resume unit test once the build environment is usable, then add
   an integration test that interrupts a real ClickHouse bootstrap after a checkpoint. Verify that
   `HEAD` is absent after any failed batch and that restart converges without logical duplicates or
   missing rows.
3. Add differential tests against `TestDB` or a temporary RocksDB for point, prefix, range, raw, and
   reference-counted reads. Include empty keys, zero bytes, large values, and ordering boundaries.
4. Validate the exact RowBinary representation and all DDL against the chosen ClickHouse release.
   The current code relies on `LowCardinality(String)` being transparent in RowBinary and on HTTP
   query parameters using the server's typed placeholder syntax.
5. Decide and implement remote retry semantics. Writes are idempotent but can span several HTTP
   requests; reads currently panic on transport or server failure to avoid returning incorrect
   misses. Define bounded retries, backoff, cancellation, and the node-level failure policy.
6. Test restart behavior while both local cold RocksDB and ClickHouse writers are active, including
   local head ahead of remote head, hot tail movement, and resharding boundaries.

### Operability and security work

1. Add ClickHouse-specific metrics instead of sharing only the existing cold-copy result metric:
   request latency, inserted rows/bytes, remote head, final-head lag, retry count, failures, bulk
   migration progress, and shadow-read mismatches.
2. Add health checks that distinguish schema/authentication failures, transport failures, query
   budget failures, and corrupt RowBinary responses.
3. Review TLS requirements, certificate trust, password-file permissions, secret rotation, user
   quotas, query limits, and least-privilege grants.
4. Define cluster topology and DDL for the actual target. The provided SQL is a single local
   `ReplacingMergeTree`, not a replicated or distributed production cluster definition.
5. Validate NVMe-to-cold movement under sustained merge load. Tune part size, insert batch size,
   ZSTD level, merge concurrency, and free-space thresholds from measured data.
6. Write backup, restore, rebuild, disk-failure, and disaster-recovery runbooks before any RocksDB
   cold data is retired.

### Storage-model work still deferred

The implementation mirrors every cold `DBCol` row into one table. This gives lookup parity, but it
does not deliver the full storage-efficiency design yet. Pending work includes:

- The RocksDB census and production query census from Phase 0.
- Normalized `blocks_by_height`, `shard_records_by_height`, `archive_locators`, `trie_objects`,
  `archive_epoch_data`, and `archive_commits` models.
- Fact builders shared with existing cloud archival code.
- Compact transaction, receipt, outcome, account, and block locators that preserve O(log n)-style
  primary-key pruning without duplicating large payloads.
- Deterministic range manifests, digests, contiguous commit tracking, and a resumable exporter for
  the normalized schema.
- Shadow-read sampling and byte-level/RPC-level comparison tooling.
- A deliberate historical-state decision. The raw implementation can copy `DBCol::State`, but the
  design requires trie latency, CPU, cache, and view-call benchmarks before accepting ClickHouse as
  the historical state backend.
- Per-domain cutover. The current `read_enabled` switch routes all eligible cold misses through
  ClickHouse; the design calls for transactional history to cut over before historical state.

## Migration from an existing archive node

The migration remains gated. The current code supplies useful parts of Phase 1 through Phase 3,
but it does not replace the evidence required by any phase.

### Phase 0: measure before designing capacity

Status: not started.

Build read-only census tooling for the existing archive RocksDB. Record physical/logical bytes,
row counts, key/value sizes, compression estimates, value histograms, duplicate payloads,
resharding-era state duplication, growth per million heights, and the real RPC/indexer query mix.
This phase determines whether the normalized model attacks the dominant bytes and prevents capacity
planning from using the 110 TiB headline alone.

Exit gate: a reproducible corpus and query census exists, and candidate ClickHouse capacity is based
on measured copies, indexes, marks, and durability overhead.

### Phase 1: prove byte parity with a raw mirror

Status: code largely implemented, proof not run.

The raw `cold_rows` table, `ClickHouseDB`, read-through split-store wiring, and finalized-height
writer implement the intended prototype. What remains is exporting a bounded representative range
to a real server and differentially comparing every supported `Store` read against cold RocksDB.

Exit gate: zero unexplained byte or deserialization mismatches across point, prefix, range, raw,
reference-counted, RPC, and indexer reads for the selected protocol eras and resharding boundaries.

### Phase 2: bulk-load the target schema

Status: raw snapshot streaming exists; normalized bulk loading is not implemented.

For an existing split archive, the current background bootstrap path bulk-streams the local cold
RocksDB while its writer remains active, checkpoints every bounded batch by last key, and commits
the captured `HEAD` and `COLD_HEAD` last. This is sufficient for raw-parity migration testing. It
does not provide
the design's deterministic height ranges, source checkpoint, normalized fact/locator tables,
manifests, digests, locator coverage, or highest-contiguous-range commit protocol.

Before production bulk load:

1. Create a consistent RocksDB checkpoint or freeze the exported range.
2. Record chain identity and boundary `H0`.
3. Export deterministic ranges with protocol-era and resharding metadata.
4. Sort large inserts by each target table's `ORDER BY` key.
5. Validate counts, bytes, digests, locator coverage, and random byte reads before committing a
   range.
6. Resume only from the highest contiguous validated range.

Exit gate: the target through `H0` is complete, reproducible, independently validated, and can
resume after interruption without trusting a partially written head.

### Phase 3: tail and shadow-read

Status: tail writer exists; shadow validation does not.

Keep the RocksDB cold loop authoritative. Start ClickHouse at `H0 + 1`, expose lag and error metrics,
and sample eligible reads against both stores without serving ClickHouse results. Exercise indexer
ranges and historical RPC calls, then increase coverage only while lag, error rate, and query
budgets remain inside agreed gates.

Exit gate: the agreed shadow period completes with zero unexplained mismatches, bounded lag, and
acceptable cold-cache and warm-cache latency under production concurrency.

### Phase 4: cut over transactional history

Status: coarse read switch exists; safe domain cutover is pending.

The `read_enabled` setting can make ClickHouse the cold side of `SplitDB`, but it routes the whole
eligible cold surface and requires a restart/config change. Implement or validate a narrower
resolver for blocks, chunks, transactions, receipts, outcomes, and state changes first. Keep the
old cold RocksDB mounted read-only and prove that the rollback switch restores it without data
conversion or ClickHouse mutation.

Exit gate: transactional archive traffic meets its SLO, rollback has been exercised, and no state
query was cut over accidentally.

### Phase 5: decide historical state separately

Status: not decided.

The raw bulk copier currently includes `DBCol::State`; that capability is not an architectural
decision. Benchmark trie-object compression, random trie fetches, batched historical view calls,
cache behavior, CPU, and concurrency. If the result misses the operational budget, keep historical
state in cold RocksDB and document the hybrid design as complete.

Exit gate: either ClickHouse historical state passes its explicit latency/CPU gates, or the hybrid
RocksDB-state boundary is accepted and implemented as a first-class configuration.

### Phase 6: retire the old cold store

Status: not started. The current code deletes nothing.

Retirement requires an agreed clean shadow period, a restore or rebuild inside the RTO, verified
durability for the intended disk-failure scenario, production capacity alarms, completed runbooks,
and an explicit operator action. Do not add automatic RocksDB deletion to the migration path.

Exit gate: the operator approves retirement after rollback, restore, rebuild, and failure drills.

## Recommended next-session order

1. Reproduce the production build and resolve the `near-test-contracts` test-environment failure.
2. Stand up a pinned local ClickHouse and add end-to-end RowBinary/schema tests.
3. Run a small RocksDB-to-ClickHouse raw migration and build a differential verifier.
4. Exercise process restart during bootstrap, including a restart within `DBCol::State`, then verify
   the persisted `CLICKHOUSE_BOOTSTRAP_PROGRESS` checkpoint and final `HEAD` directly in ClickHouse.
5. Add migration and lag metrics before increasing the corpus size.
6. Implement Phase 0 census tooling and run it against a representative archive copy.
7. Benchmark the raw schema and the normalized design from `DESIGNDOC.md` on the same corpus.
8. Decide whether to keep raw parity as an operational fallback only or retain selected raw rows in
   the final model.
9. Implement deterministic normalized-range export, manifests, and locators only after the data
   justifies them.
10. Run the Phase 3 shadow program before enabling production reads.
11. Treat transactional history and historical state as separate cutover decisions.

## Useful commands

Run the production compilation after any Rust change:

```text
cargo fmt
cargo check -p near-store -p near-client -p nearcore --message-format short
```

Run focused tests using the repository-required form:

```text
cargo test --package near-store --features test_features -- db::clickhouse::tests::rowbinary_string_roundtrip --exact --show-output
cargo test --package near-store --features test_features -- db::clickhouse::tests::rowbinary_long_string_roundtrip --exact --show-output
cargo test --package near-store --features test_features -- archive::clickhouse::config::tests::rejects_sql_in_identifiers --exact --show-output
cargo test --package near-store --features test_features -- archive::cold_storage::test::test_copy_all_cold_data_resumes_from_last_key --exact --show-output
```

Do not run clippy unless a commit is about to be created. When the user asks for a commit, use the
exact repository command:

```text
RUSTFLAGS="-D warnings" cargo clippy --all-features --all-targets
```

Do not commit until the user asks. Before committing, inspect all unrelated working-tree changes and
exclude user-owned files from the commit.
