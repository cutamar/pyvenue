from __future__ import annotations

import pytest

from pyvenue.domain.commands import Cancel, PlaceLimit
from pyvenue.domain.events import Event, OrderRejected, TopOfBookChanged, TradeOccurred
from pyvenue.domain.types import AccountId, Instrument, OrderId, Price, Qty, Side
from pyvenue.venue import Venue
from utils import venue_with_balances

BTC = Instrument("BTC-USD")
ETH = Instrument("ETH-USD")


def _pl(
    inst: Instrument, oid: str, side: Side, price: int, qty: int, client_ts_ns: int
) -> PlaceLimit:
    return PlaceLimit(
        instrument=inst,
        account_id=AccountId("alice"),
        order_id=OrderId(oid),
        side=side,
        price=Price(price),
        qty=Qty(qty),
        client_ts_ns=client_ts_ns,
    )


def _cx(inst: Instrument, oid: str, client_ts_ns: int) -> Cancel:
    return Cancel(
        instrument=inst,
        account_id=AccountId("alice"),
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
    v = venue_with_balances(
        [BTC, ETH],
        {
            BTC: {"alice": {"USD": 999999, "BTC": 999999}},
            ETH: {"alice": {"USD": 999999, "ETH": 999999}},
        },
    )

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
    v = venue_with_balances(
        [BTC],
        {
            BTC: {"alice": {"USD": 999999, "BTC": 999999}},
        },
    )

    ev = v.submit(_pl(ETH, "x1", Side.BUY, 1, 1, 1))
    assert len(ev) == 1
    assert isinstance(ev[0], OrderRejected)
    assert "instrument" in ev[0].reason.lower()


def test_global_seq_monotonic_across_instruments() -> None:
    """
    Milestone 4 design choice: Venue is the sequencing authority.
    Events emitted for ETH after BTC must have larger seq numbers.
    """
    v = venue_with_balances(
        [BTC, ETH],
        {
            BTC: {"alice": {"USD": 999999, "BTC": 999999}},
            ETH: {"alice": {"USD": 999999, "ETH": 999999}},
        },
    )

    ev1 = v.submit(_pl(BTC, "b1", Side.BUY, 100, 1, 1))
    s1 = _max_seq(ev1)

    ev2 = v.submit(_pl(ETH, "a1", Side.SELL, 200, 1, 2))
    s2 = _max_seq(ev2)

    assert s2 > s1


def test_trade_occurs_only_within_same_instrument() -> None:
    v = venue_with_balances(
        [BTC, ETH],
        {
            BTC: {"alice": {"USD": 999999, "BTC": 999999}},
            ETH: {"alice": {"USD": 999999, "ETH": 999999}},
        },
    )

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
    v = venue_with_balances(
        [BTC, ETH],
        {
            BTC: {"alice": {"USD": 999999, "BTC": 999999}},
            ETH: {"alice": {"USD": 999999, "ETH": 999999}},
        },
    )

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


def test_cancel_routes_to_correct_instrument_only() -> None:
    v = venue_with_balances(
        [BTC, ETH],
        {
            BTC: {"alice": {"USD": 999999, "BTC": 999999}},
            ETH: {"alice": {"USD": 999999, "ETH": 999999}},
        },
    )

    # Same order_id on both instruments (should be allowed)
    v.submit(_pl(BTC, "o1", Side.BUY, 100, 1, 1))
    v.submit(_pl(ETH, "o1", Side.BUY, 50, 1, 2))

    # Cancel only BTC order
    ev = v.submit(_cx(BTC, "o1", 3))
    assert "OrderCanceled" in _types(ev)

    # BTC book removed
    assert v.engines[BTC].book.best_bid() is None

    # ETH book still has its bid
    assert v.engines[ETH].book.best_bid() == 50


def test_order_ids_can_collide_across_instruments() -> None:
    """
    Routing should namespace order_id by instrument.
    Same order_id should be valid on different books.
    """
    v = venue_with_balances(
        [BTC, ETH],
        {
            BTC: {"alice": {"USD": 999999, "BTC": 999999}},
            ETH: {"alice": {"USD": 999999, "ETH": 999999}},
        },
    )

    ev1 = v.submit(_pl(BTC, "dup", Side.BUY, 100, 1, 1))
    ev2 = v.submit(_pl(ETH, "dup", Side.SELL, 200, 1, 2))

    assert "OrderAccepted" in _types(ev1)
    assert "OrderAccepted" in _types(ev2)

    assert v.engines[BTC].book.best_bid() == 100
    assert v.engines[ETH].book.best_ask() == 200


def test_cancel_unknown_order_rejected_per_instrument() -> None:
    """
    Cancel should be evaluated in the target instrument namespace,
    not globally.
    """
    v = venue_with_balances(
        [BTC, ETH],
        {
            BTC: {"alice": {"USD": 999999, "BTC": 999999}},
            ETH: {"alice": {"USD": 999999, "ETH": 999999}},
        },
    )

    # Place on BTC only
    v.submit(_pl(BTC, "o1", Side.BUY, 100, 1, 1))

    # Cancel same id on ETH should reject (unknown in ETH)
    ev = v.submit(_cx(ETH, "o1", 2))
    assert len(ev) == 1
    assert isinstance(ev[0], OrderRejected)
    assert "unknown" in ev[0].reason.lower()


def test_replay_rejects_events_for_unconfigured_instrument() -> None:
    """
    If you replay into a Venue that doesn't include an instrument that appears
    in the event stream, replay should fail loudly (or you can choose to ignore,
    but then adjust this test).
    """
    v = venue_with_balances(
        [BTC, ETH],
        {
            BTC: {"alice": {"USD": 999999, "BTC": 999999}},
            ETH: {"alice": {"USD": 999999, "ETH": 999999}},
        },
    )

    events: list[Event] = []
    events.extend(v.submit(_pl(BTC, "b1", Side.BUY, 100, 1, 1)))
    events.extend(v.submit(_pl(ETH, "a1", Side.SELL, 200, 1, 2)))

    # Replay with only BTC configured should error
    with pytest.raises(RuntimeError):
        Venue.replay(instruments=[BTC], events=events, rebuild_book=True)
