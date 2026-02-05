from __future__ import annotations

from pyvenue.domain.commands import Cancel, PlaceLimit
from pyvenue.domain.types import AccountId, Instrument, OrderId, Price, Qty, Side
from pyvenue.engine.engine import Engine
from pyvenue.engine.state import EngineState
from utils import FixedClock, NextMeta


def test_determinism_same_commands_same_events():
    cmds = [
        PlaceLimit(
            instrument=Instrument("BTC-USD"),
            account_id=AccountId("alice"),
            order_id=OrderId("o1"),
            side=Side.BUY,
            price=Price(100),
            qty=Qty(5),
            client_ts_ns=1,
        ),
        Cancel(
            instrument=Instrument("BTC-USD"),
            account_id=AccountId("alice"),
            order_id=OrderId("o1"),
            client_ts_ns=2,
        ),
    ]

    e1 = Engine(instrument=Instrument("BTC-USD"), next_meta=NextMeta(FixedClock(999)))
    e2 = Engine(instrument=Instrument("BTC-USD"), next_meta=NextMeta(FixedClock(999)))

    out1 = [ev for c in cmds for ev in e1.submit(c)]
    out2 = [ev for c in cmds for ev in e2.submit(c)]

    assert out1 == out2
    assert [ev.seq for ev in out1] == [1, 2, 3, 4, 5]


def test_replay_events_rebuilds_same_state():
    engine = Engine(
        instrument=Instrument("BTC-USD"), next_meta=NextMeta(FixedClock(111))
    )
    engine.submit(
        PlaceLimit(
            instrument=Instrument("BTC-USD"),
            account_id=AccountId("alice"),
            order_id=OrderId("o1"),
            side=Side.SELL,
            price=Price(200),
            qty=Qty(3),
            client_ts_ns=1,
        )
    )
    engine.submit(
        Cancel(
            instrument=Instrument("BTC-USD"),
            account_id=AccountId("alice"),
            order_id=OrderId("o1"),
            client_ts_ns=2,
        )
    )

    events = engine.log.all()

    rebuilt = EngineState()
    rebuilt.apply_all(events)

    orig = engine.state.orders[OrderId("o1")]
    new = rebuilt.orders[OrderId("o1")]

    assert orig.instrument == new.instrument
    assert orig.side == new.side
    assert orig.price == new.price
    assert orig.qty == new.qty
    assert orig.remaining == new.remaining
    assert orig.status == new.status


def test_reject_duplicate_order_id():
    engine = Engine(instrument=Instrument("BTC-USD"), next_meta=NextMeta(FixedClock(1)))
    engine.submit(
        PlaceLimit(
            instrument=Instrument("BTC-USD"),
            account_id=AccountId("alice"),
            order_id=OrderId("o1"),
            side=Side.BUY,
            price=Price(100),
            qty=Qty(1),
            client_ts_ns=1,
        )
    )
    events = engine.submit(
        PlaceLimit(
            instrument=Instrument("BTC-USD"),
            account_id=AccountId("alice"),
            order_id=OrderId("o1"),
            side=Side.BUY,
            price=Price(101),
            qty=Qty(1),
            client_ts_ns=2,
        )
    )
    assert events[0].type == "OrderRejected"


def test_reject_instrument_mismatch():
    engine = Engine(instrument=Instrument("BTC-USD"), next_meta=NextMeta(FixedClock(1)))
    events = engine.submit(
        PlaceLimit(
            instrument=Instrument("ETH-USD"),
            account_id=AccountId("alice"),
            order_id=OrderId("o2"),
            side=Side.BUY,
            price=Price(100),
            qty=Qty(1),
            client_ts_ns=1,
        )
    )
    assert len(events) == 1
    assert events[0].type == "OrderRejected"
    assert events[0].reason == "instrument mismatch"
