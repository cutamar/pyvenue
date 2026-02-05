from __future__ import annotations

from collections.abc import Iterable

from pyvenue.domain.commands import Cancel, PlaceLimit
from pyvenue.domain.events import Event
from pyvenue.domain.types import AccountId, Instrument, OrderId, Price, Qty, Side
from pyvenue.engine.engine import Engine
from utils import FixedClock, NextMeta

# ----------------------------
# Test helpers
# ----------------------------

INSTR = Instrument("BTC-USD")


def types(events: Iterable[Event]) -> list[str]:
    return [e.type for e in events]


def trades(events: Iterable[Event]) -> list[Event]:
    return [e for e in events if e.type == "TradeOccurred"]


def assert_trade(
    ev: Event,
    *,
    taker: str,
    maker: str,
    price_ticks: int,
    qty_lots: int,
) -> None:
    assert ev.type == "TradeOccurred"
    assert ev.instrument == INSTR
    assert ev.taker_order_id == OrderId(taker)
    assert ev.maker_order_id == OrderId(maker)
    assert ev.price == Price(price_ticks)
    assert ev.qty == Qty(qty_lots)


def place(
    engine: Engine,
    *,
    oid: str,
    side: Side,
    price: int,
    qty: int,
    client_ts_ns: int,
) -> list[Event]:
    return engine.submit(
        PlaceLimit(
            instrument=INSTR,
            account_id=AccountId("alice"),
            order_id=OrderId(oid),
            side=side,
            price=Price(price),
            qty=Qty(qty),
            client_ts_ns=client_ts_ns,
        )
    )


def cancel(engine: Engine, *, oid: str, client_ts_ns: int) -> list[Event]:
    return engine.submit(
        Cancel(
            instrument=INSTR,
            account_id=AccountId("alice"),
            order_id=OrderId(oid),
            client_ts_ns=client_ts_ns,
        )
    )


def test_single_match_full_fill_trade_at_maker_price():
    """
    Resting ask: a1 5 @ 100
    Incoming buy: b1 5 @ 110
    Expect: trade 5 @ 100 (maker price)
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta(FixedClock(1)))

    ev1 = place(e, oid="a1", side=Side.SELL, price=100, qty=5, client_ts_ns=1)
    assert types(ev1) == ["OrderAccepted", "OrderRested", "TopOfBookChanged"]

    ev2 = place(e, oid="b1", side=Side.BUY, price=110, qty=5, client_ts_ns=2)
    assert types(ev2) == ["OrderAccepted", "TradeOccurred", "TopOfBookChanged"]
    assert_trade(ev2[1], taker="b1", maker="a1", price_ticks=100, qty_lots=5)


def test_partial_fill_resting_order_keeps_remaining():
    """
    Resting ask: a1 10 @ 100
    Incoming buy: b1 4 @ 100 -> fills 4, leaves 6 on a1
    Incoming buy: b2 6 @ 100 -> fills remaining 6 on a1
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta(FixedClock(1)))

    place(e, oid="a1", side=Side.SELL, price=100, qty=10, client_ts_ns=1)

    ev_b1 = place(e, oid="b1", side=Side.BUY, price=100, qty=4, client_ts_ns=2)
    assert types(ev_b1) == ["OrderAccepted", "TradeOccurred"]
    assert_trade(ev_b1[1], taker="b1", maker="a1", price_ticks=100, qty_lots=4)

    ev_b2 = place(e, oid="b2", side=Side.BUY, price=100, qty=6, client_ts_ns=3)
    assert types(ev_b2) == ["OrderAccepted", "TradeOccurred", "TopOfBookChanged"]
    assert_trade(ev_b2[1], taker="b2", maker="a1", price_ticks=100, qty_lots=6)


def test_sweep_multiple_price_levels_and_leave_remainder():
    """
    Asks:
      a1 3 @ 100
      a2 4 @ 101
      a3 5 @ 102
    Incoming buy:
      b1 10 @ 102

    Expect trades:
      3 @ 100 (a1)
      4 @ 101 (a2)
      3 @ 102 (a3)   -> a3 remaining becomes 2

    Then another buy b2 2 @ 999 should fill the remaining 2 @ 102 from a3.
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta(FixedClock(1)))

    place(e, oid="a1", side=Side.SELL, price=100, qty=3, client_ts_ns=1)
    place(e, oid="a2", side=Side.SELL, price=101, qty=4, client_ts_ns=2)
    place(e, oid="a3", side=Side.SELL, price=102, qty=5, client_ts_ns=3)

    ev_b1 = place(e, oid="b1", side=Side.BUY, price=102, qty=10, client_ts_ns=4)
    assert types(ev_b1) == [
        "OrderAccepted",
        "TradeOccurred",
        "TradeOccurred",
        "TradeOccurred",
        "TopOfBookChanged",
    ]

    assert_trade(ev_b1[1], taker="b1", maker="a1", price_ticks=100, qty_lots=3)
    assert_trade(ev_b1[2], taker="b1", maker="a2", price_ticks=101, qty_lots=4)
    assert_trade(ev_b1[3], taker="b1", maker="a3", price_ticks=102, qty_lots=3)

    ev_b2 = place(e, oid="b2", side=Side.BUY, price=999, qty=2, client_ts_ns=5)
    assert types(ev_b2) == ["OrderAccepted", "TradeOccurred", "TopOfBookChanged"]
    assert_trade(ev_b2[1], taker="b2", maker="a3", price_ticks=102, qty_lots=2)


def test_fifo_within_price_level():
    """
    Two asks at the same price must fill FIFO.

    Asks:
      a1 3 @ 100  (first)
      a2 3 @ 100  (second)
    Incoming buy:
      b1 4 @ 100

    Expect:
      fill a1 for 3
      then a2 for 1
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta(FixedClock(1)))

    place(e, oid="a1", side=Side.SELL, price=100, qty=3, client_ts_ns=1)
    place(e, oid="a2", side=Side.SELL, price=100, qty=3, client_ts_ns=2)

    ev_b1 = place(e, oid="b1", side=Side.BUY, price=100, qty=4, client_ts_ns=3)
    assert types(ev_b1) == ["OrderAccepted", "TradeOccurred", "TradeOccurred"]

    assert_trade(ev_b1[1], taker="b1", maker="a1", price_ticks=100, qty_lots=3)
    assert_trade(ev_b1[2], taker="b1", maker="a2", price_ticks=100, qty_lots=1)


def test_no_cross_order_rests_and_can_trade_later():
    """
    If a limit order doesn't cross, it must rest.

    Place ask at 100.
    Place buy at 99 -> no trade (should rest as best bid at 99).
    Then place sell at 99 -> should trade against resting buy at 99, price=99 (maker price).
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta(FixedClock(1)))

    place(e, oid="a1", side=Side.SELL, price=100, qty=5, client_ts_ns=1)

    ev_b1 = place(e, oid="b1", side=Side.BUY, price=99, qty=1, client_ts_ns=2)
    assert types(ev_b1) == ["OrderAccepted", "OrderRested", "TopOfBookChanged"]
    assert trades(ev_b1) == []

    ev_s1 = place(e, oid="s1", side=Side.SELL, price=99, qty=1, client_ts_ns=3)
    assert types(ev_s1) == ["OrderAccepted", "TradeOccurred", "TopOfBookChanged"]
    assert_trade(ev_s1[1], taker="s1", maker="b1", price_ticks=99, qty_lots=1)


def test_cancel_prevents_fill():
    """
    Cancelled resting orders must not be matched.

    Place ask a1 5@100
    Cancel a1
    Place buy b1 5@100 -> should NOT trade with a1
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta(FixedClock(1)))

    place(e, oid="a1", side=Side.SELL, price=100, qty=5, client_ts_ns=1)

    ev_c = cancel(e, oid="a1", client_ts_ns=2)
    assert types(ev_c) == ["OrderCanceled", "TopOfBookChanged"]

    ev_b1 = place(e, oid="b1", side=Side.BUY, price=100, qty=5, client_ts_ns=3)
    assert types(ev_b1)[0] == "OrderAccepted"
    assert trades(ev_b1) == []


def test_sequence_numbers_are_monotonic():
    """
    Basic sanity: seq should increase by 1 for each emitted event.
    (We don't care about timestamps yet; FixedClock returns a constant.)
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta(FixedClock(123)))

    events: list[Event] = []
    events += place(e, oid="a1", side=Side.SELL, price=100, qty=2, client_ts_ns=1)  # 1
    events += place(e, oid="b1", side=Side.BUY, price=100, qty=1, client_ts_ns=2)  # 2,3
    events += cancel(
        e, oid="a1", client_ts_ns=3
    )  # maybe 4 (if a1 still active), or reject if already filled

    seqs = [ev.seq for ev in events]
    assert seqs == list(range(seqs[0], seqs[0] + len(seqs)))
