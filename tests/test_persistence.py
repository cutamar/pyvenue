from __future__ import annotations

from pathlib import Path

from pyvenue.persistence.event_store import JsonlEventStore
from pyvenue.persistence.recovery import recover_venue
from pyvenue.persistence.snapshot_store import JsonSnapshotStore

from pyvenue.domain.commands import PlaceLimit, PlaceMarket
from pyvenue.domain.types import (
    AccountId,
    Instrument,
    OrderId,
    Price,
    Qty,
    Side,
    TimeInForce,
)
from pyvenue.venue import Venue

BTC = Instrument("BTC-USD")
ETH = Instrument("ETH-USD")


def _pl_limit(
    inst: Instrument,
    acct: str,
    oid: str,
    side: Side,
    price: int,
    qty: int,
    ts: int,
    tif: TimeInForce = TimeInForce.GTC,
) -> PlaceLimit:
    return PlaceLimit(
        instrument=inst,
        account_id=AccountId(acct),
        order_id=OrderId(oid),
        side=side,
        price=Price(price),
        qty=Qty(qty),
        client_ts_ns=ts,
        tif=tif,
        post_only=False,
    )


def _pl_mkt(
    inst: Instrument, acct: str, oid: str, side: Side, qty: int, ts: int
) -> PlaceMarket:
    return PlaceMarket(
        instrument=inst,
        account_id=AccountId(acct),
        order_id=OrderId(oid),
        side=side,
        qty=Qty(qty),
        client_ts_ns=ts,
    )


def _seed_balances(v: Venue) -> None:
    # Test-driven API: you already have credit/available/held in state.
    v.state.credit(AccountId("alice"), v.assets.quote(BTC), 1_000_000)
    v.state.credit(AccountId("bob"), v.assets.base(BTC), 100)
    v.state.credit(AccountId("alice"), v.assets.quote(ETH), 1_000_000)
    v.state.credit(AccountId("bob"), v.assets.base(ETH), 100)


def _venue_snapshot(v: Venue) -> tuple:
    """
    Minimal stable snapshot for comparison in tests.
    Keep this small: we only need to know recovery produced the same result.

    Test-driven expectation:
      - v.state.digest() returns a deterministic tuple/hashable representation
      - v.books_digest() returns deterministic top-of-book + resting counts per instrument
    """
    return (v.seq, v.state.digest(), v.books_digest())


def test_eventstore_roundtrip_and_recover_matches_in_memory(tmp_path: Path) -> None:
    """
    Write events to disk, recover a new Venue, and compare:
      - seq
      - state digest (balances/holds/orders)
      - books digest (top-of-book & resting counts)
    """
    event_path = tmp_path / "events.jsonl"
    snap_dir = tmp_path / "snaps"
    snap_dir.mkdir()

    store = JsonlEventStore(event_path)
    snaps = JsonSnapshotStore(snap_dir)

    v = Venue(instruments=[BTC, ETH])
    _seed_balances(v)

    all_events = []

    # BTC: bob rests sell, alice market buys partially
    all_events.extend(v.submit(_pl_limit(BTC, "bob", "btc_s1", Side.SELL, 100, 5, 1)))
    all_events.extend(v.submit(_pl_mkt(BTC, "alice", "btc_mb1", Side.BUY, 3, 2)))

    # ETH: bob rests sell, alice buys
    all_events.extend(v.submit(_pl_limit(ETH, "bob", "eth_s1", Side.SELL, 200, 4, 3)))
    all_events.extend(v.submit(_pl_mkt(ETH, "alice", "eth_mb1", Side.BUY, 4, 4)))

    store.append(all_events)

    # Recover into a fresh instance from disk (no snapshot yet)
    r = recover_venue(
        instruments=[BTC, ETH],
        event_store=store,
        snapshot_store=snaps,
    )

    assert _venue_snapshot(r) == _venue_snapshot(v)


def test_recovery_uses_snapshot_then_replays_tail(tmp_path: Path) -> None:
    """
    Ensure snapshot is used and only tail is replayed.
    This test expects recover_venue to expose some debug stats.
    """
    event_path = tmp_path / "events.jsonl"
    snap_dir = tmp_path / "snaps"
    snap_dir.mkdir()

    store = JsonlEventStore(event_path)
    snaps = JsonSnapshotStore(snap_dir)

    v = Venue(instruments=[BTC, ETH])
    _seed_balances(v)

    # Generate a history
    events1 = []
    events1.extend(v.submit(_pl_limit(BTC, "bob", "s1", Side.SELL, 100, 10, 1)))
    events1.extend(v.submit(_pl_mkt(BTC, "alice", "mb1", Side.BUY, 4, 2)))
    store.append(events1)

    # Save snapshot at this point (seq should be max of events so far)
    # Test-driven API: venue can produce Snapshot object.
    snap = v.snapshot()
    snaps.save(snap)

    # Add more tail events after snapshot
    events2 = []
    events2.extend(v.submit(_pl_limit(ETH, "bob", "s2", Side.SELL, 200, 3, 3)))
    events2.extend(v.submit(_pl_mkt(ETH, "alice", "mb2", Side.BUY, 2, 4)))
    store.append(events2)

    # Recover should load snapshot and replay only events after snapshot.seq
    r, stats = recover_venue(
        instruments=[BTC, ETH],
        event_store=store,
        snapshot_store=snaps,
        return_stats=True,
    )

    assert _venue_snapshot(r) == _venue_snapshot(v)

    # Stats expectations (test-driven):
    assert stats.loaded_snapshot is True
    assert stats.snapshot_seq == snap.seq
    assert stats.replayed_events > 0
    assert stats.replayed_from_seq == snap.seq + 1


def test_eventstore_ignores_trailing_partial_line(tmp_path: Path) -> None:
    """
    Optional robustness:
    If the process crashes mid-write, the last line in JSONL may be truncated.
    EventStore should ignore the trailing partial line (or raise a controlled error).
    This test expects it to ignore.
    """
    event_path = tmp_path / "events.jsonl"
    store = JsonlEventStore(event_path)

    # Write a valid line then a truncated line
    event_path.write_text(
        '{"type":"Dummy","seq":1,"ts_ns":1}\n{"type":"Dummy"', encoding="utf-8"
    )

    # iter_from should not blow up; should yield the first event only
    events = list(store.iter_from(1))
    assert len(events) == 1
    assert events[0].seq == 1
