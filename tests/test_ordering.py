"""Ordering, concurrency and no-refetch guarantees."""

from __future__ import annotations

import threading
import time

import httpx

from app.db import BlockRecord, TransferRecord
from app.ingest import Ingestor
from app.source_client import ChainSourceClient, SourceError
from tests.conftest import Stack


class FixedHeadClient(ChainSourceClient):
    """A source client whose first head read is captured, then returns `pinned`.

    Blocks are still fetched for real, which is exactly the shape of a slow
    round: the head was seen at revision N, the source moved on, and the round
    finishes late.
    """

    def __init__(self, real: ChainSourceClient, pinned):
        self.real = real
        self.pinned = pinned
        self.served_pinned = False
        self.base_url = real.base_url

    def get_head(self):
        if not self.served_pinned:
            self.served_pinned = True
            return self.pinned
        return self.real.get_head()

    def get_block(self, block_hash):
        return self.real.get_block(block_hash)

    def close(self):
        return None


def _block_counts(stack: Stack) -> dict[str, int]:
    return dict(
        stack.source_client().get("/admin/stats").json()["block_requests_by_hash"]
    )


def test_ordinary_advance_never_refetches_known_prefix(synced: Stack):
    stack = synced
    before = _block_counts(stack)
    stack.source_client().post("/admin/advance", json={"count": 3})
    report = stack.api().post("/admin/sync").json()
    assert report["outcome"] == "committed"
    assert report["changed_blocks"] == 3
    assert report["fetched_blocks"] == 3
    after = _block_counts(stack)
    for block_hash in before:
        assert after.get(block_hash, 0) == before[block_hash]
    # The previous head is reused for ancestor detection, not refetched.
    prev_head = stack.source_client().get("/admin/canonical").json()["blocks"][5]
    assert after.get(prev_head["hash"], 0) == before.get(prev_head["hash"], 0)


def test_reorg_back_to_abandoned_branch_reuses_cached_blocks(synced: Stack):
    stack = synced
    source = stack.source
    old_canonical = dict(source.canonical)
    stack.source_client().post(
        "/admin/fork", json={"from_height": 3, "count": 2, "transfers_per_block": 2}
    )
    assert stack.api().post("/admin/sync").json()["outcome"] == "committed"
    counts = _block_counts(stack)

    # Re-org back onto the original blocks by re-seeding the branch from the
    # cached chain: fork from height 3 with the original hashes restored.
    with source.lock:
        source.canonical = dict(old_canonical)
        source.head_hash = old_canonical[max(old_canonical)]
        source.revision += 1
        source._save()
    report = stack.api().post("/admin/sync").json()
    assert report["outcome"] == "committed"
    assert report["fetched_blocks"] == 0, "cached abandoned blocks were refetched"
    after = _block_counts(stack)
    for block_hash, count in counts.items():
        assert after.get(block_hash, 0) == count


def test_genuinely_late_round_is_rejected_after_newer_publish(synced: Stack):
    stack = synced
    stale = stack.source_client().get("/head").json()
    pinned = type(
        "H",
        (),
        {
            "revision": stale["revision"],
            "height": stale["height"],
            "hash": stale["hash"],
        },
    )()
    wrapper = FixedHeadClient(
        stack.api_servers[0].server.config.app.state.ingestor.source, pinned
    )
    late_ingestor = Ingestor(
        stack.api_servers[0].server.config.app.state.db,
        wrapper,
        instance_id="late-instance",
        verify_head_after_round=False,
    )

    # Newer revision gets published first, from the real source.
    stack.source_client().post("/admin/advance", json={"count": 3})
    assert stack.api().post("/admin/sync").json()["outcome"] == "committed"
    fresh = stack.api().get("/transfers", params={"address": "addr-alice"}).json()

    # The late round now tries to publish revision=stale on a stale head.
    report = late_ingestor.run_round()
    assert report.outcome == "stale"
    assert report.published is False
    after = stack.api().get("/transfers", params={"address": "addr-alice"}).json()
    assert after["chain_view"] == fresh["chain_view"]
    assert after["head_basis"] == fresh["head_basis"]


def test_concurrent_instances_cannot_regress_view(synced: Stack):
    stack = synced
    stack.add_api("instance-b")
    stopped = threading.Event()

    def hammer(index: int):
        while not stopped.is_set():
            stack.api(index).post("/admin/sync")

    threads = [threading.Thread(target=hammer, args=(i,)) for i in (0, 1)]
    for thread in threads:
        thread.start()
    try:
        for i in range(4):
            stack.source_client().post("/admin/advance", json={"count": 2})
            time.sleep(0.15)
        # Let both loops settle on the final head.
        time.sleep(0.5)
        last_expected = stack.source_client().get("/head").json()
        deadline = time.time() + 10
        while time.time() < deadline:
            views = [
                stack.api(i).get("/transfers", params={"address": "addr-alice"}).json()
                for i in (0, 1)
            ]
            if all(
                v["head_basis"]["revision"] == last_expected["revision"] for v in views
            ):
                break
            time.sleep(0.1)
        else:
            raise AssertionError("instances did not converge on the newest revision")
        assert views[0]["chain_view"] == views[1]["chain_view"]
        assert views[0]["head_basis"]["height"] == last_expected["height"]
        revisions = [v["head_basis"]["revision"] for v in views]
        assert revisions[0] == revisions[1] == last_expected["revision"]
    finally:
        stopped.set()
        for thread in threads:
            thread.join(15)


def test_head_rollback_then_recovery_keeps_monotonic_revision(synced: Stack):
    stack = synced
    first = stack.api().get("/transfers", params={"address": "addr-alice"}).json()
    stack.source_client().post("/admin/rollback", json={"to_height": 3})
    rolled = stack.api().post("/admin/sync").json()
    assert rolled["outcome"] == "committed"
    rolled_view = stack.api().get("/transfers", params={"address": "addr-alice"}).json()
    assert rolled_view["head_basis"]["revision"] > first["head_basis"]["revision"]
    stack.source_client().post("/admin/advance", json={"count": 4})
    recovered = stack.api().post("/admin/sync").json()
    assert recovered["outcome"] == "committed"
    final = stack.api().get("/transfers", params={"address": "addr-alice"}).json()
    assert final["head_basis"]["height"] == 7
    assert final["head_basis"]["revision"] > rolled_view["head_basis"]["revision"]
    assert {b["height"] for b in final["chain_view"]["blocks"]} == set(range(8))


def test_no_publish_when_head_unchanged_revision_equal(synced: Stack):
    stack = synced
    report = stack.api().post("/admin/sync").json()
    assert report["outcome"] == "stale"
    assert report["published"] is False


def test_restart_during_partial_round_does_not_lose_or_refetch(synced: Stack):
    """A cached-but-unpublished suffix is reused after a crash/restart.

    We reproduce the crash artifact directly: the new head block was fetched
    and cached, but the round never published (process died before commit).
    """
    stack = synced
    stack.source_client().post("/admin/advance", json={"count": 3})
    head = stack.source_client().get("/head").json()
    head_block = stack.source_client().get(f"/blocks/{head['hash']}").json()
    db = stack.api_servers[0].server.config.app.state.db
    db.cache_block(
        BlockRecord(
            hash=head_block["hash"],
            height=head_block["height"],
            parent_hash=head_block["parent_hash"],
            transfers=tuple(
                TransferRecord(
                    txid=t["txid"],
                    from_address=t["from_address"],
                    to_address=t["to_address"],
                    amount=t["amount"],
                )
                for t in head_block["transfers"]
            ),
        ),
        head["revision"],
    )
    # The published view did NOT advance: cache is ahead of the view.
    before = stack.api().get("/transfers", params={"address": "addr-alice"}).json()
    assert before["head_basis"]["height"] == 5

    stack.api_servers[0].stop()
    stack.api_servers.clear()
    stack.add_api("instance-after-crash")
    counts_before = _block_counts(stack)
    report = stack.api().post("/admin/sync").json()
    assert report["outcome"] == "committed"
    assert report["changed_blocks"] == 3
    # The cached head block is reused; the other two are fetched.
    assert report["reused_blocks"] >= 1
    assert report["fetched_blocks"] == 2
    counts_after = _block_counts(stack)
    assert counts_after[head["hash"]] == counts_before[head["hash"]]
    view = stack.api().get("/transfers", params={"address": "addr-alice"}).json()
    assert view["head_basis"]["height"] == 8
    assert [b["height"] for b in view["chain_view"]["blocks"]] == list(range(9))


# ------------------------------------------------------- shared status ordering
class OrderingSourceClient(ChainSourceClient):
    """Wraps a real source client with a test-controlled head sequence.

    The first head read is served from ``queue``; later reads defer to the
    real source (unless ``always`` is set, which pins every read).  Blocks are
    fetched for real.
    """

    def __init__(self, real: ChainSourceClient, queue=None, always=None):
        self.real = real
        self.queue = list(queue or [])
        self.always = always
        self.base_url = real.base_url

    def get_head(self):
        if self.always is not None:
            return self.always
        if self.queue:
            return self.queue.pop(0)
        return self.real.get_head()

    def get_block(self, block_hash):
        return self.real.get_block(block_hash)

    def close(self):
        return None


def _head_tuple(client: httpx.Client) -> dict:
    return client.get("/head").json()


def _head_obj(payload: dict):
    from app.db import ObservedHead

    return ObservedHead(
        revision=payload["revision"], height=payload["height"], hash=payload["hash"]
    )


def test_late_failure_after_newer_publish_cannot_degrade(synced: Stack):
    """A slow round that started earlier must not overwrite a newer success.

    Realistic shape: round A reads the head, stalls inside a block pull; round
    B (started later) publishes a newer revision; round A's pull then fails.
    Round A's failure is stale by start order and must be discarded, leaving
    the newer successful status in place.
    """
    stack = synced
    api = stack.api()
    db = stack.api_servers[0].server.config.app.state.db
    real_source = stack.api_servers[0].server.config.app.state.ingestor.source

    # The source advances; A observes this head.
    stack.source_client().post("/admin/advance", json={"count": 3})
    head_a = _head_obj(_head_tuple(stack.source_client()))

    release = threading.Event()
    entered = threading.Event()

    class StallingClient(OrderingSourceClient):
        def get_block(self, block_hash):
            entered.set()
            assert release.wait(10), "test did not release the stalled pull"
            raise SourceError("simulated transient block pull failure")

    slow = Ingestor(
        db,
        StallingClient(real_source, always=head_a),
        instance_id="slow-round",
        verify_head_after_round=False,
    )
    result: dict = {}

    def run_slow():
        result["report"] = slow.run_round().as_dict()

    thread = threading.Thread(target=run_slow)
    thread.start()
    try:
        assert entered.wait(10), "slow round never started pulling blocks"
        # Round B starts after A and publishes an even newer revision.
        stack.source_client().post("/admin/advance", json={"count": 2})
        assert stack.api().post("/admin/sync").json()["outcome"] == "committed"
        fresh = api.get("/transfers", params={"address": "addr-alice"}).json()
        assert fresh["head_basis"]["height"] == 10
        assert api.get("/chain/status").json()["status"] == "ok"

        release.set()
        thread.join(15)
    finally:
        release.set()
        thread.join(15)

    assert result["report"]["outcome"] == "failed"
    status = api.get("/chain/status").json()
    # Round A started earlier than the successful round B: its late failure
    # cannot turn the newer success back into degraded.
    assert status["status"] == "ok", status
    assert status["last_error"] is None
    assert status["head"]["height"] == 10
    assert status["last_result_seq"] >= 1
    after = api.get("/transfers", params={"address": "addr-alice"}).json()
    assert after["chain_view"] == fresh["chain_view"]
    assert after["head_basis"] == fresh["head_basis"]


def test_failure_without_head_read_marks_degraded_and_recovery_clears_it(
    synced: Stack,
):
    """A failing round that cannot read the head still degrades the shared view.

    Once the source recovers, reading the *same* published revision/height/hash
    clears the degraded status without republishing.
    """
    stack = synced
    api = stack.api()
    before = api.get("/chain/status").json()
    assert before["status"] == "ok"

    # The head endpoint fails before any head is observed.
    stack.source_client().post("/admin/faults", json={"head_failures": 3})
    report = stack.api().post("/admin/sync").json()
    assert report["outcome"] == "failed"
    assert report["observed_head"] is None

    status = api.get("/chain/status").json()
    assert status["status"] == "degraded"
    assert status["serving"] is True
    assert status["last_error"]
    assert status["last_error_at"]
    # The published view itself is untouched and still queryable.
    view = api.get("/transfers", params={"address": "addr-alice"}).json()
    assert view["head_basis"]["height"] == 5
    assert view["chain_view"]["blocks"]

    # Source recovers; the head is exactly the published one.
    stack.source_client().post("/admin/faults", json={})
    before_recovery = api.get("/transfers", params={"address": "addr-alice"}).json()
    report = stack.api().post("/admin/sync").json()
    assert report["outcome"] == "stale", report
    assert report["published"] is False
    status = api.get("/chain/status").json()
    assert status["status"] == "ok"
    assert status["last_error"] is None
    assert status["last_error_at"] is None
    # Same-head confirmation must not republish: the view, revision, head and
    # original publication time are unchanged; only the round timestamp moves.
    after = api.get("/transfers", params={"address": "addr-alice"}).json()
    assert after["chain_view"] == before_recovery["chain_view"]
    for key in ("revision", "height", "hash", "published_at"):
        assert after["head_basis"][key] == before_recovery["head_basis"][key], key
    assert status["head"]["height"] == 5

    # A lower revision is still skipped (no resets of a newer published view).
    stale = _head_tuple(stack.source_client())
    stack.source_client().post("/admin/advance", json={"count": 2})
    assert stack.api().post("/admin/sync").json()["outcome"] == "committed"
    newer = api.get("/transfers", params={"address": "addr-alice"}).json()
    db = stack.api_servers[0].server.config.app.state.db
    real_source = stack.api_servers[0].server.config.app.state.ingestor.source
    late = Ingestor(
        db,
        OrderingSourceClient(real_source, always=_head_obj(stale)),
        instance_id="lower-revision",
        verify_head_after_round=False,
    )
    report = late.run_round().as_dict()
    assert report["outcome"] == "stale"
    assert report["published"] is False
    final = api.get("/transfers", params={"address": "addr-alice"}).json()
    assert final["chain_view"] == newer["chain_view"]
    assert final["head_basis"]["revision"] == newer["head_basis"]["revision"]


def test_restart_keeps_shared_status_ordering_and_serves_address_queries(
    synced: Stack,
):
    """Restarting an instance resumes ordering state and serves the view."""
    stack = synced
    stack.source_client().post("/admin/faults", json={"head_failures": 1})
    assert stack.api().post("/admin/sync").json()["outcome"] == "failed"
    degraded = stack.api().get("/chain/status").json()
    assert degraded["status"] == "degraded"
    seq_before = degraded["round_counter"]

    stack.api_servers[0].stop()
    stack.api_servers.clear()
    stack.add_api("instance-after-restart")

    resumed = stack.api().get("/chain/status").json()
    assert resumed["round_counter"] >= seq_before
    assert resumed["status"] == "degraded", "status was not persisted across restart"
    assert resumed["last_error"]
    # Address queries keep using the most recently published full view.
    view = stack.api().get("/transfers", params={"address": "addr-alice"}).json()
    assert view["head_basis"]["height"] == 5
    assert [b["height"] for b in view["chain_view"]["blocks"]] == list(range(6))

    # And the follower still pulls blocks and publishes normally.
    stack.source_client().post("/admin/faults", json={})
    stack.source_client().post("/admin/advance", json={"count": 2})
    report = stack.api().post("/admin/sync").json()
    assert report["outcome"] == "committed"
    assert report["fetched_blocks"] == 2
    recovered = stack.api().get("/chain/status").json()
    assert recovered["status"] == "ok"
    assert recovered["last_error"] is None


def test_older_same_head_confirmation_does_not_clear_newer_degraded(
    synced: Stack,
):
    """Ordering applies to recovery too: an earlier round cannot clear a newer
    round's recorded failure.  A subsequently-started round still can."""
    stack = synced
    api = stack.api()
    db = stack.api_servers[0].server.config.app.state.db
    real_source = stack.api_servers[0].server.config.app.state.ingestor.source

    # Round A reserves an early sequence number but has not finished.
    early = Ingestor(
        db,
        OrderingSourceClient(
            real_source, always=_head_obj(_head_tuple(stack.source_client()))
        ),
        instance_id="early-round",
        verify_head_after_round=False,
    )
    early_report = early.run_round()

    # A later round fails without a view change, recording degraded.
    stack.source_client().post("/admin/faults", json={"head_failures": 1})
    assert stack.api().post("/admin/sync").json()["outcome"] == "failed"
    assert api.get("/chain/status").json()["status"] == "degraded"
    stack.source_client().post("/admin/faults", json={})

    # Force the same-head confirmation path with the early sequence number.
    from app.ingest import RoundReport

    report = RoundReport(
        round_id="early-confirm",
        instance_id="early-round",
        outcome="",
        round_seq=early_report.round_seq,
    )
    head = _head_obj(_head_tuple(stack.source_client()))
    result = early._sync_to(head, report)
    assert result.outcome == "superseded"
    status = api.get("/chain/status").json()
    assert status["status"] == "degraded"
    assert status["last_error"]

    # A later-started round observing the same head does clear it.
    report2 = RoundReport(round_id="new-confirm", instance_id="x", outcome="")
    report2.round_seq = db.begin_round()
    result2 = early._sync_to(head, report2)
    assert result2.outcome == "stale"
    assert result2.published is False
    status = api.get("/chain/status").json()
    assert status["status"] == "ok"
    assert status["last_error"] is None


def test_schema_migration_adds_ordering_columns(tmp_path):
    """A database created by an older build is upgraded in place on startup."""
    import sqlite3

    from app.db import Database

    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE sync_state (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            published_revision INTEGER NOT NULL DEFAULT 0,
            head_height INTEGER NOT NULL DEFAULT -1,
            head_hash TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'empty',
            published_at TEXT,
            last_round_at TEXT,
            last_error TEXT,
            last_error_at TEXT,
            rounds INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO sync_state (id, published_revision, head_height, head_hash,
                                status, rounds)
        VALUES (1, 7, 6, 'ab', 'ok', 3);
        """
    )
    conn.commit()
    conn.close()

    db = Database(str(path))
    db.initialize()
    state = db.get_state()
    assert state.round_counter == 0
    assert state.last_result_seq == 0
    assert state.published_revision == 7
    assert db.begin_round() == 1
    assert db.begin_round() == 2

    # The legacy view is still served.
    snapshot = db.snapshot(address="addr-alice")
    assert snapshot.state.head_height == 6
