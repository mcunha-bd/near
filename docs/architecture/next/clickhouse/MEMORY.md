# NEAR ClickHouse archive implementation handoff

This file is the implementation handoff for a future Codex session running on another machine. It
records what exists in the working tree, what was verified, what remains provisional, and how the
migration from an existing archive node must proceed. Read [DESIGNDOC.md](DESIGNDOC.md) for the
architecture and [README.md](README.md) for the current provisioning instructions.

## Resume here

1. Read the repository `AGENTS.md` before changing code. In particular, do not run the protocol
   schema check or update OpenAPI unless the user explicitly asks. Run `cargo fmt` after Rust edits,
   and run the repository-wide clippy command only immediately before a requested commit.
2. Inspect the working tree before editing. The implementation was developed on `master` from base
   commit `82983fe95e9ac41503998b42b607d37e19ff2939` and was not committed. The next machine may have
   a different base or additional user changes, so treat this hash as historical context rather
   than a checkout instruction.
3. Preserve unrelated user-owned changes. At the time of this handoff, `.gitignore` was modified and
   `.cgcignore` was untracked before the ClickHouse work started. Do not overwrite or remove them.
4. Review the current diff and confirm that the ClickHouse files are still uncommitted or were
   transferred into the new checkout. Do not assume Cargo caches, a ClickHouse binary, or a running
   ClickHouse server exist on the next machine.
5. Keep the phase boundary explicit: the working code is a raw key/value parity backend. The
   normalized block, shard, locator, trie, epoch, and commit tables in the design document are not
   implemented yet.

## Current implementation status

The current increment gives nearcore a lossless ClickHouse cold backend while retaining the exact
RocksDB keys, values, Borsh encodings, and existing `DBCol` lookup behavior. It is intended to prove
integration correctness before committing to the more normalized and storage-efficient target
model.

### ClickHouse database backend

[core/store/src/db/clickhouse.rs](../../../../core/store/src/db/clickhouse.rs) contains
`ClickHouseDB`, an implementation of near-store's synchronous `Database` trait.

It currently covers:

- HTTP access through `reqwest::blocking::Client`, including connect and request timeouts.
- Optional basic authentication with the password read from a file relative to the neard home.
- Startup validation against `system.columns` so an absent or incompatible raw table fails fast.
- RowBinary inserts that preserve binary keys and values without JSON or hexadecimal value storage.
- Configurable request splitting by approximate bytes and row count.
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
read/write switches, timeouts, and insert limits. Identifier validation prevents database or table
names from being interpolated as arbitrary SQL.

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
RocksDB and ClickHouse writers can run concurrently.

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
- Existing split archive: bulk-stream the completed local cold RocksDB snapshot first, then resume
  finalized-height writes from its head through a local hot+cold split source.

`core/store/src/archive/cold_storage.rs::copy_all_cold_data` implements the second path. It streams
cold columns in bounded transactions and deliberately writes `HEAD` last. An interrupted import has
no committed target head and can restart idempotently. `ClickHouseDB` stops accepting writes after
the first latched error, which prevents a partially failed bulk import from writing `HEAD` later in
the same attempt.

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

The following commands passed after the final Rust changes:

```text
cargo fmt --check
cargo check -p near-store -p near-client -p nearcore --message-format short
git diff --check
```

The focused unit test was attempted with the repository-required command:

```text
cargo test --package near-store --features test_features -- db::clickhouse::tests::rowbinary_string_roundtrip --exact --show-output
```

It did not reach the selected test. The unrelated `near-test-contracts` build was SIGKILLed while
building the embedded `wasm32-unknown-unknown` standard library (`libc`, then `object`). Retrying
with `CARGO_BUILD_JOBS=2` failed at the same external build step. Treat this as an unresolved test
environment/resource issue, not as a passing test and not as a ClickHouse test failure.

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
2. Test `copy_all_cold_data` with an interrupted bulk import. Verify that `HEAD` is absent after any
   failed batch and that restart converges without logical duplicates or missing rows.
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

For an existing split archive, the current startup path bulk-streams the local cold RocksDB snapshot
and commits its `HEAD` last. This is sufficient for raw-parity migration testing. It does not provide
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
4. Add migration and lag metrics before increasing the corpus size.
5. Implement Phase 0 census tooling and run it against a representative archive copy.
6. Benchmark the raw schema and the normalized design from `DESIGNDOC.md` on the same corpus.
7. Decide whether to keep raw parity as an operational fallback only or retain selected raw rows in
   the final model.
8. Implement deterministic normalized-range export, manifests, and locators only after the data
   justifies them.
9. Run the Phase 3 shadow program before enabling production reads.
10. Treat transactional history and historical state as separate cutover decisions.

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
```

Do not run clippy unless a commit is about to be created. When the user asks for a commit, use the
exact repository command:

```text
RUSTFLAGS="-D warnings" cargo clippy --all-features --all-targets
```

Do not commit until the user asks. Before committing, inspect all unrelated working-tree changes and
exclude user-owned files from the commit.
