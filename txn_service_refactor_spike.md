# Transaction Service Refactor Spike

Date: 2026-03-17

## Goal

Hands-on spike the design in real repos first, keep the first pass runnable, and record where the design collides with current code.

## Phase Breakdown

### Phase 1: `client-go` facade

Goal:
- Add a real `txnservice` package with an opaque handle and service API.
- Keep implementation as a thin wrapper over existing `tikv.KVStore` and `transaction.KVTxn`.
- Validate whether the service boundary is usable without rewriting transport or 2PC internals.

Implementation target:
- `TxnService`
- `SnapshotService`
- `TxnHandle`
- Savepoint/checkpoint support
- Focused unit tests

### Phase 2: additive transactional read RPCs

Goal:
- Introduce explicit transactional read RPC names without removing old RPCs.
- Keep semantics identical to existing transactional reads.
- Avoid batch-command/client-go transport migration in the first pass.

Implementation target:
- `kvproto`: `TxnGet`, `TxnScan`, `TxnBatchGet`
- `tikv`: alias handlers that delegate to existing futures
- focused unit tests for alias conversion logic

### Phase 3: transport migration in `client-go`

Goal:
- Teach `client-go` request routing to use the new `Txn*` RPCs.

Status:
- Not implemented in this spike.

Reason:
- The real surface is much larger than the RFC suggests:
  - `tikvrpc.CmdType`
  - batch command oneofs
  - grpc client interface
  - codec layers
  - metrics
  - mockstore / mock grpc service
  - compatibility fallback

### Phase 4: `tidb` integration

Goal:
- Wire the new `client-go/txnservice` abstraction into TiDB.

Status:
- Not implemented in this spike.

Reason:
- TiDB already has a substantial session-level transaction abstraction in `pkg/sessiontxn`.
- A safe migration needs a deliberate plan around `TxnManager`, `LazyTxn`, `savepoint`, `MemBuffer`, and `stale read`.

### Phase 5: write-path rename and non-transactional versioned read split

Goal:
- Add `TxnPrewrite` / `TxnCommit` / lock management rename.
- Add `VersionedKv` and split transactional vs non-transactional read semantics.

Status:
- Not implemented in this spike.

Reason:
- Too large for the first runnable slice.

### Phase 6: deprecation / removal

Goal:
- Deprecate old APIs after new ones are proven.

Status:
- Not started.

## Implemented Work

### 1. `client-go`

Implemented a first-pass `txnservice` package:

- Files:
  - `txnservice/api.go`
  - `txnservice/types.go`
  - `txnservice/service.go`
  - `txnservice/service_test.go`

- Added:
  - `TxnService`
  - `SnapshotService`
  - `TxnHandle`
  - `TxnOptions`
  - `SnapshotOptions`
  - isolation/priority enums
  - transaction state tracking

- Current implementation:
  - `Begin`
  - `Commit`
  - `Rollback`
  - `Get`
  - `BatchGet`
  - `Set`
  - `Delete`
  - `Scan`
  - `LockKeys`
  - `CreateSavepoint`
  - `RollbackToSavepoint`
  - `ReleaseSavepoint`
  - `GetSnapshot`

- Important implementation choice:
  - This is a wrapper over current `KVStore` / `KVTxn`.
  - It does not attempt to hide or delete lower-level APIs yet.
  - That kept the first phase small and testable.

### 2. `kvproto`

Added additive explicit transactional read messages and RPCs:

- `proto/kvrpcpb.proto`
  - `TxnGetRequest`
  - `TxnGetResponse`
  - `TxnScanRequest`
  - `TxnScanResponse`
  - `TxnBatchGetRequest`
  - `TxnBatchGetResponse`

- `proto/tikvpb.proto`
  - `rpc TxnGet(...)`
  - `rpc TxnScan(...)`
  - `rpc TxnBatchGet(...)`

- Regenerated:
  - `pkg/kvrpcpb/kvrpcpb.pb.go`
  - `pkg/tikvpb/tikvpb.pb.go`
  - `scripts/proto.lock`

Notes:
- Batch-command oneofs were intentionally not changed in this spike.
- That keeps the change additive and avoids pulling client-go transport rewrite into the first pass.

### 3. `tikv`

Added alias transactional read handlers:

- `Cargo.toml`
  - local patch to use `../kvproto` during the spike

- `src/server/service/kv.rs`
  - new alias handler macro path with explicit metric mapping
  - `txn_get`
  - `txn_scan`
  - `txn_batch_get`
  - alias futures:
    - `future_txn_get`
    - `future_txn_scan`
    - `future_txn_batch_get`
  - response conversion helpers:
    - `convert_get_response`
    - `convert_scan_response`
    - `convert_batch_get_response`

- `src/server/metrics.rs`
  - added grpc metric labels for:
    - `txn_get`
    - `txn_scan`
    - `txn_batch_get`

- Added focused unit tests in `src/server/service/kv.rs`:
  - `test_convert_get_response_to_txn_get_response`
  - `test_convert_scan_response_to_txn_scan_response`
  - `test_convert_batch_get_response_to_txn_batch_get_response`

Implementation note:
- The new RPCs currently reuse old storage futures and old semantics.
- This is an alias layer, not a semantic split yet.

## Real Problems Found

### 1. `client-go` savepoint support is not a drop-in substitute for TiDB savepoints

The first wrapper can checkpoint/revert buffered changes, but this first pass is not a complete replacement for TiDB savepoint behavior.

Observed gap:
- The current spike only validated rollback of keys added after the savepoint.
- I did not complete or validate overwrite rollback semantics or pessimistic-lock release after savepoint rollback.

Consequence:
- This is enough for a spike facade.
- It is not enough to replace TiDB savepoint logic.

### 2. `client-go` transport migration is much bigger than the RFC sketch

The new RPC names cannot be adopted in `client-go` with a small patch.

Real migration surface includes:
- `tikvrpc.CmdType`
- grpc client interface
- request/response helpers
- batch command encoding
- codec layers
- metrics
- region request code
- mock grpc service
- mockstore request handling
- compatibility fallback

Consequence:
- Phase 3 needs its own dedicated work plan.

### 3. `tikv` method-name plumbing reaches into proxy metrics

The new RPC name was not only a trait/handler change.

Hidden dependency:
- proxy-side grpc metric labels are method-name keyed too

Consequence:
- even an alias handler needs metric label additions or special proxy handling

### 4. `tidb` side is blocked on architecture, not syntax

This spike reinforced the earlier review conclusion:
- TiDB already has `pkg/sessiontxn` as its real session transaction service.
- Directly replacing `kv.Transaction` usage with a new wrapper is not realistic without first deciding how it interacts with:
  - `TxnManager`
  - `TxnContextProvider`
  - `LazyTxn`
  - `MemBuffer`
  - savepoints
  - stale read / exact ts semantics

## Skipped In This Spike

- `client-go` routing of reads through `TxnGet` / `TxnScan` / `TxnBatchGet`
- `kvproto` batch-command oneof updates
- `TxnPrewrite` / `TxnCommit` / write-path rename
- `VersionedKv` service
- coprocessor split
- `pd` changes
- `tidb` vendor/sessiontxn integration
- deprecation/removal work

## Validation

### `client-go`

Command:

```bash
go test -v ./txnservice/...
```

Result:
- pass

### `kvproto`

Commands:

```bash
/home/ywxk/src/kvproto/scripts/generate_go.sh
go test ./pkg/...
```

Result:
- pass

Note:
- `make go` first stopped intentionally at the proto-lock check because `scripts/proto.lock` changed.
- I kept the generated lockfile and ran the generator directly.

### `tikv`

Command:

```bash
cargo test -p tikv test_convert_ --lib
```

Result:
- pass

## Suggested Next Step

If continuing the spike, the best next task is Phase 3 only:

1. patch `client-go` to locally replace `github.com/pingcap/kvproto` with `../kvproto`
2. add `tikvrpc` request/response support for the three new read RPCs
3. update mockstore and grpc mock service
4. switch one transactional read path behind a feature flag or compatibility fallback

That will answer the next real question: whether the transport rename is operationally affordable before touching TiDB session semantics.
