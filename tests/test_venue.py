from __future__ import annotations

from pyvenue.domain.commands import Cancel, PlaceLimit
from pyvenue.domain.events import Event, OrderRejected, TopOfBookChanged, TradeOccurred
from pyvenue.domain.types import Instrument, OrderId, Price, Qty, Side
from pyvenue.venue import Venue

BTC = Instrument("BTC-USD")
ETH = Instrument("ETH-USD")


def _pl(
    inst: Instrument, oid: str, side: Side, price: int, qty: int, client_ts_ns: int
) -> PlaceLimit:
    return PlaceLimit(
        instrument=inst,
        order_id=OrderId(oid),
        side=side,
        price=Price(price),
        qty=Qty(qty),
        client_ts_ns=client_ts_ns,
    )


def _cx(inst: Instrument, oid: str, client_ts_ns: int) -> Cancel:
    return Cancel(
        instrument=inst,
        order_id=OrderId(oid),
        client_ts_ns=client_ts_ns,
    )


def _types(events: list[Event]) -> list[str]:
    return [e.__class__.__name__ for e in events]


def _bbo(events: list[Event]) -> list[TopOfBookChanged]:
    return [e for e in events if isinstance(e, TopOfBookChanged)]


def _trades(events: list[Event]) -> list[TradeOccurred]:
    return [e for e in events if isinstance(e, TradeOccurred)]


def _max_seq(events: list[Event]) -> int:
    return max((e.seq for e in events), default=0)


def test_routes_to_correct_instrument_and_isolates_books() -> None:
    v = Venue(instruments=[BTC, ETH])

    # Place a BTC bid; should not affect ETH
    ev_btc = v.submit(_pl(BTC, "b1", Side.BUY, 100, 1, 1))
    assert "OrderAccepted" in _types(ev_btc)
    assert _bbo(ev_btc)  # BBO changed on BTC
    assert v.engines[BTC].book.best_bid() == 100
    assert v.engines[BTC].book.best_ask() is None

    assert v.engines[ETH].book.best_bid() is None
    assert v.engines[ETH].book.best_ask() is None

    # Place an ETH ask; should not affect BTC
    ev_eth = v.submit(_pl(ETH, "a1", Side.SELL, 200, 1, 2))
    assert "OrderAccepted" in _types(ev_eth)
    assert _bbo(ev_eth)  # BBO changed on ETH
    assert v.engines[ETH].book.best_ask() == 200

    # BTC unchanged
    assert v.engines[BTC].book.best_bid() == 100
    assert v.engines[BTC].book.best_ask() is None


def test_unknown_instrument_rejected() -> None:
    v = Venue(instruments=[BTC])

    ev = v.submit(_pl(ETH, "x1", Side.BUY, 1, 1, 1))
    assert len(ev) == 1
    assert isinstance(ev[0], OrderRejected)
    assert "instrument" in ev[0].reason.lower()


def test_global_seq_monotonic_across_instruments() -> None:
    """
    Milestone 4 design choice: Venue is the sequencing authority.
    Events emitted for ETH after BTC must have larger seq numbers.
    """
    v = Venue(instruments=[BTC, ETH])

    ev1 = v.submit(_pl(BTC, "b1", Side.BUY, 100, 1, 1))
    s1 = _max_seq(ev1)

    ev2 = v.submit(_pl(ETH, "a1", Side.SELL, 200, 1, 2))
    s2 = _max_seq(ev2)

    assert s2 > s1


def test_trade_occurs_only_within_same_instrument() -> None:
    v = Venue(instruments=[BTC, ETH])

    # Rest BTC ask
    v.submit(_pl(BTC, "a1", Side.SELL, 100, 2, 1))

    # Rest ETH ask at same price; must not match with BTC bid later
    v.submit(_pl(ETH, "a1", Side.SELL, 100, 2, 2))

    # Cross in BTC only
    ev = v.submit(_pl(BTC, "b1", Side.BUY, 200, 2, 3))
    trades = _trades(ev)

    assert (
        len(trades) == 1 or len(trades) == 2
    )  # depending on whether you emit 1 trade per fill or aggregate
    for t in trades:
        assert t.instrument == BTC
        assert t.maker_order_id == OrderId("a1")


def test_replay_reconstructs_multiple_books() -> None:
    """
    Venue.replay should reconstruct state+books for ALL instruments.
    This assumes you implement:
      Venue.replay(instruments=[...], events=[...], rebuild_books=True)
    and the replay rebuild is usable (cancel works, matching works).
    """
    v = Venue(instruments=[BTC, ETH])

    all_events: list[Event] = []

    # BTC: maker rests, then partially filled
    all_events.extend(v.submit(_pl(BTC, "m1", Side.SELL, 100, 5, 1)))
    all_events.extend(v.submit(_pl(BTC, "t1", Side.BUY, 200, 2, 2)))

    # ETH: resting bid, then canceled
    all_events.extend(v.submit(_pl(ETH, "b1", Side.BUY, 50, 1, 3)))
    all_events.extend(v.submit(_cx(ETH, "b1", 4)))

    r = Venue.replay(instruments=[BTC, ETH], events=all_events)

    # BTC should still have a resting ask at 100 (remaining 3)
    assert r.engines[BTC].book.best_ask() == 100
    assert r.engines[BTC].book.best_bid() is None

    # ETH should be empty (bid was canceled)
    assert r.engines[ETH].book.best_bid() is None
    assert r.engines[ETH].book.best_ask() is None

    # Prove replayed books are usable: cancel remaining BTC maker should succeed
    ev_c = r.submit(_cx(BTC, "m1", 10))
    assert "OrderCanceled" in _types(ev_c)
    assert r.engines[BTC].book.best_ask() is None
