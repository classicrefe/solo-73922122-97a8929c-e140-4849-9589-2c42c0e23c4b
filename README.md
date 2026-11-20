# Confirmed transfers — chain-following delivery API

Delivers **confirmed** address transfers to address-behaviour analysis, keeping
the published chain view consistent with the chain source: reorgs and head
rollbacks withdraw abandoned and insufficiently confirmed transfers, and a slow
or late ingestion round can never overwrite a newer published view.

Two FastAPI + SQLite instances share one volume, plus a controllable local HTTP
chain source. Delivered as launchable Docker Compose.

## Quick start

```bash
docker compose up --build
```

| Service        | URL                          | Purpose                        |
| -------------- | ---------------------------- | ------------------------------ |
| API instance A | http://localhost:8000/docs   | Swagger: query + force a round |
| API instance A | http://localhost:8000/       | browser demo console           |
| API instance B | http://localhost:8001/docs   | second instance, same volume   |
| Chain source   | http://localhost:9100/docs   | advance / fork / rollback / faults |

Both API instances mount the same named volume `api-data` at `/data`; the
source persists to `source-data`. Restarting any container resumes from the
persisted chain view and progress — the chain is never re-collected.

Demo (UI or curl):

```bash
# force one complete ingestion round (also happens automatically each second)
curl -X POST http://localhost:8000/admin/sync

# query confirmed transfers for an address
curl 'http://localhost:8000/transfers?address=addr-alice'

# cause a reorg: replace heights 4..6 of the source chain
curl -X POST http://localhost:9100/admin/fork \
  -H 'content-type: application/json' \
  -d '{"from_height":4,"count":3,"transfers_per_block":2,"salt":"demo"}'
curl -X POST http://localhost:8000/admin/sync
curl 'http://localhost:8000/transfers?address=addr-alice'   # moved head + withdrawn keys

# roll the head back (withdraws even without new blocks)
curl -X POST http://localhost:9100/admin/rollback \
  -H 'content-type: application/json' -d '{"to_height":3}'
curl -X POST http://localhost:8000/admin/sync

# simulate source failures: the published view stays served, status degrades
curl -X POST http://localhost:9100/admin/faults \
  -H 'content-type: application/json' -d '{"head_failures":2,"block_failures":2}'
curl -X POST http://localhost:8000/admin/sync
curl http://localhost:8000/chain/status      # status=degraded, last_error set
curl 'http://localhost:8001/transfers?address=addr-alice'   # replica B still serves

# recovery: clear the faults; reading the unchanged head restores ok without
# republishing (published_at and the view stay the same)
curl -X POST http://localhost:9100/admin/faults \
  -H 'content-type: application/json' -d '{}'
curl -X POST http://localhost:8000/admin/sync   # outcome=stale, but status=ok
curl http://localhost:8000/chain/status         # status=ok, last_error=null
```

Watch both replicas at once (`:8000/docs` and `:8001/docs`, or the consoles at
`/`): a round started on A that is still pulling blocks when B publishes a
newer head can fail late without turning B's `ok` back into `degraded` —
`/chain/status` exposes `round_counter` (last start sequence) and
`last_result_seq` (start sequence of the applied result) so the ordering is
visible. `docker compose logs -f api-a api-b` shows each attempt.

Local (no Docker), Python 3.11+:

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q                      # 30 end-to-end tests

# terminals 1-3
SOURCE_STATE_PATH=/tmp/chain-source.json INITIAL_BLOCKS=6 \
  uvicorn chain_source.main:app --port 9100
DB_PATH=/tmp/chain.db CHAIN_SOURCE_URL=http://127.0.0.1:9100 INSTANCE_ID=api-a \
  uvicorn app.main:app --port 8000
DB_PATH=/tmp/chain.db CHAIN_SOURCE_URL=http://127.0.0.1:9100 INSTANCE_ID=api-b \
  uvicorn app.main:app --port 8001
```

## Delivery contract

* **Only confirmed transfers are served.** Confirmations are computed against
  the **published** head: `confirmations = head_height - block_height + 1`.
  The default minimum is 3, i.e. a transfer is served only when it is **at least
  two blocks below the published head**. The floor is enforced in config, the
  DB query, the Pydantic model and the HTTP query parameter — clients can raise
  it but never lower it (`?min_confirmations=2` → `422`).
* **Every address query returns the same complete chain view and its basis.**
  `GET /transfers?address=…` returns `chain_view` (view id, head, every block
  from genesis through head with hash/parent/tx_count), `head_basis` (the
  **published revision** the view is based on, head height/hash, timestamps),
  the confirmation policy, the address's confirmed transfers, and counts. Two
  different addresses in the same revision get byte-identical chain views.
* **Reorgs and rollbacks withdraw.** Abandoned-suffix blocks leave the
  `chain_blocks` view; and because confirmations are recomputed from the
  published head on every query, a rollback withdraws transfers that are still
  canonical but now too shallow **even if no block was fetched**. Withdrawal is
  global: a txid gone from the view is gone from every address answer.
* **Late rounds cannot overwrite newer revisions.** The observed head is fixed
  once per round. `commit_view` runs in one SQLite `BEGIN IMMEDIATE`
  transaction that compares the target revision against `published_revision`
  and refuses (`stale`) when a revision at least as new is already published.
* **Shared status is ordered by round start.** Every round reserves a
  monotonic `round_counter` value in `sync_state` **before** its first source
  request, and records `last_result_seq` when its outcome is applied. A failure
  from a round that started before the round whose result is already recorded
  is discarded instead of overwriting it — so when instance A is still in a
  slow block pull while instance B has already published a newer head, A's
  late failure cannot turn B's `ok` back into `degraded`. Both sequence
  numbers are exposed on `/chain/status`. Successful publishes remain
  revision-guarded only, since the revision guard alone keeps the view
  monotonic.
* **Failures keep the published view, and surface.** Blocks are fetched into an
  immutable cache first; the published view and progress only change in the
  final commit. Any head/block/validation error leaves the previous view intact
  and, if a view exists, records `status=degraded` plus `last_error` /
  `last_error_at` on `/chain/status` and `/admin/rounds` — **even when the
  round never managed to read the head**. Before the first publish the status
  stays `empty`.
* **Recovery restores `ok`.** A successful publish clears the error and sets
  `ok`. So does a round whose head read is exactly the published revision,
  height and hash: it is a same-head re-confirmation, so nothing is
  re-published (`published_at` and the view are untouched) and a lower
  revision is still skipped as `stale`. This is what clears a lingering
  `degraded` after a transient source outage once the source is back on the
  already-published head.
* **No refetch of the known prefix.** The follower walks the two heads down
  `parent_hash` to the common ancestor, cache first. Only changed-suffix blocks
  are fetched; abandoned branches stay cached and are reused if a branch
  returns. Blocks pulled before a crash are reused after restart.
* **Slow pulls do not block queries.** Normal API handlers are synchronous
  functions (threadpool), and the follower runs in its own thread on its own
  SQLite connections. A delayed source only delays the next commit.
* **Persisted view + progress, restart-safe.** `sync_state` (published revision,
  head, status, errors, round-counter/last-result ordering) and
  `chain_blocks` are written together in the same transaction. On restart the
  follower continues from the persisted head with the same ordering state, and
  address queries keep serving the most recently published complete view
  while blocks are pulled normally. Older databases are migrated in place
  (`round_counter`/`last_result_seq` columns are added on startup).
* **Two instances, one view.** Both replicas may run the follower. A SQLite
  lease (renewed during long rounds) elects one active follower at a time; the
  revision guard keeps the view monotonic and the round-start ordering keeps
  either replica's late failure from overwriting the other's newer status, even
  when both rounds overlap.

## API

| Method | Path                       | Description |
| ------ | -------------------------- | ----------- |
| GET    | `/transfers?address=…`     | Confirmed transfers + full chain view + head basis |
| GET    | `/chain/blocks`            | Full published chain view + head basis |
| GET    | `/chain/status`            | Progress: revision, head, errors, lease, follower state |
| GET    | `/health`                  | Ops health for Compose |
| POST   | `/admin/sync`              | Run exactly one round synchronously and report it |
| GET    | `/admin/rounds`            | Recent round audit log |
| GET    | `/docs`                    | Swagger UI |
| GET    | `/`                        | Browser demo console |

The demo console has buttons for a round, an address query, advance, fork,
rollback, failure injection and slow-source delay. It calls source controls
through the same-origin `/demo/source/*` passthrough.

### Chain source

Blocks are immutable and fetched by hash; `/head` carries a monotonically
increasing `revision`. Control endpoints:

| Method | Path                | Description |
| ------ | ------------------- | ----------- |
| GET    | `/head`             | `{revision, height, hash}` |
| GET    | `/blocks/{hash}`    | `{height, hash, parent_hash, transfers[]}` |
| GET    | `/canonical/{height}` | Canonical block at height |
| POST   | `/admin/advance`    | Append `count` blocks |
| POST   | `/admin/fork`       | Replace the canonical suffix above `from_height` |
| POST   | `/admin/rollback`   | Move the head down to `to_height` |
| POST   | `/admin/faults`     | `delay_ms`, `head_failures`, `block_failures`, `fail_all` |
| GET    | `/admin/stats`      | Per-block request counts (proves no refetching) |
| GET    | `/docs`             | Swagger UI |

A transfer is `{txid, from_address, to_address, amount}` with an integer
`amount`; `from`/`to` aliases are also emitted.

## Persistence schema

* `blocks` / `block_transfers` — immutable by-hash cache of every block ever
  fetched, including abandoned branches.
* `chain_blocks` — the published canonical height→hash view.
* `sync_state` — published revision, head, status, last round/error, round count,
  and the round-start ordering counters (`round_counter`, `last_result_seq`).
* `ingest_lease` — one active follower across instances.
* `ingest_rounds` — bounded audit trail (last 500 rounds).

## Verified behaviour (tests)

`pytest` runs 30 end-to-end tests against real HTTP servers on ephemeral
ports and a real SQLite file. They cover: the two-block confirmation cutoff and
that it cannot be lowered; every address seeing the identical complete view;
reorg suffix-only update with no common-prefix refetch (asserted from
`/admin/stats` per-hash counters); global withdrawal of abandoned txids; head
rollback withdrawing both abandoned and now-too-shallow transfers; failures
leaving the published view intact; slow pulls not delaying queries; genuinely
late rounds rejected after a newer publish; two instances converging from a
shared volume; restart resuming without refetching; crash-cached blocks reused;
same-head revision bumps; source state persistence; and `/docs`/console
availability.

The shared-status regressions have dedicated tests:

* a round that started earlier, stalled in a block pull and *then* failed after
  a later-started round published a newer head does not degrade the shared
  status or alter either view;
* a failing round that could not read the head at all still marks an existing
  view `degraded` with `last_error`, while queries keep serving the previous
  complete view;
* the source returning the exact published revision/height/hash clears
  `degraded` and the error without republishing (view, revision and
  `published_at` unchanged), and a lower revision is still skipped;
* a restart keeps the persisted ordering counters and degraded status, still
  answers address queries from the published view, and the same process then
  fetches and publishes new blocks normally;
* an earlier-started same-head confirmation cannot clear a later round's
  recorded failure, while a later-started confirmation can;
* a database created by the previous schema is migrated in place (ordering
  columns added, existing view still served).

## Known limits

* SQLite with a shared WAL is intended for replicas on the **same host**
  (one Compose volume). It is not a multi-node database; use a real DB server
  for cross-host replicas.
* One global lease means only one follower is actively pulling at a time; the
  other replica serves reads and takes over after the lease expires
  (`INGEST_LEASE_SECONDS`, default 15 s).
* The chain source is a deterministic simulation, not a Bitcoin/EVM node; its
  "blocks" and txids are synthetic and its revision is local to the process
  state file.
* Confirmation depth counts blocks on top of the transaction's block inclusive
  of the head, per the documented formula; there is no separate finality rule.
* The demo console is intentionally unauthenticated and bound to the host's
  published ports; keep it off untrusted networks.
* Docker was unavailable in the build sandbox, so the Compose definition and
  Dockerfiles were validated by YAML inspection and by running both ASGI apps
  under Uvicorn with the exact pinned requirements, not by an actual
  `docker compose up`. See "Verification performed" below.

## Verification performed

| Check | Result |
| --- | --- |
| `pytest` (30 end-to-end tests, real HTTP servers + real SQLite file) | pass (5 consecutive full runs) |
| The three new shared-status regression tests against the pre-fix code | fail as expected (`degraded` overwrote `ok` / late failure applied); pass after the fix |
| Both ASGI apps under Uvicorn with the pinned dependencies (`chain_source.main:app`, `app.main:app`) | started, served `/health`, `/docs`, `/`, followed source advances, forks and faults over real sockets |
| Two instances sharing one SQLite file | identical `/chain/blocks` views; only one held the lease; failed instance kept serving |
| Live double-instance ordering check | a delayed round on A started first, B published a newer head, A's late round did not overwrite it; both views identical afterwards |
| Live failure/recovery check | `head_failures` with no head read → `status=degraded` + error, replica B still served; faults cleared → same-head round `outcome=stale`, `status=ok`, `published_at` unchanged |
| Restart continuation | persisted view served before any round; subsequent rounds fetched only new blocks (per-hash counters on the source unchanged for known blocks) |
| `docker compose up --build` | **not executed** — no Docker daemon in this environment; Compose/Dockerfiles reviewed only |

Not implemented / out of scope: authentication and rate limiting on the API and
demo console, a real chain node adapter, cross-host replica coordination,
metrics/tracing, and pruning of the immutable block cache.
