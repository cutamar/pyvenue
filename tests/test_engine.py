from __future__ import annotations

from dataclasses import dataclass

from pyvenue.domain.commands import Cancel, PlaceLimit
from pyvenue.domain.types import Instrument, OrderId, Price, Qty, Side
from pyvenue.engine.engine import Engine
from pyvenue.engine.state import EngineState
from pyvenue.infra.clock import Clock


@dataclass(frozen=True, slots=True)
class FixedClock(Clock):
    t: int = 123

    def now_ns(self) -> int:
        return self.t


def test_determinism_same_commands_same_events():
    cmds = [
        PlaceLimit(
            instrument=Instrument("BTC-USD"),
            order_id=OrderId("o1"),
            side=Side.BUY,
            price=Price(100),
            qty=Qty(5),
            client_ts_ns=1,
        ),
        Cancel(instrument=Instrument("BTC-USD"), order_id=OrderId("o1"), client_ts_ns=2),
    ]

    e1 = Engine(clock=FixedClock(999))
    e2 = Engine(clock=FixedClock(999))

    out1 = [ev for c in cmds for ev in e1.submit(c)]
    out2 = [ev for c in cmds for ev in e2.submit(c)]

    assert out1 == out2
    assert [ev.seq for ev in out1] == [1, 2]


def test_replay_events_rebuilds_same_state():
    engine = Engine(clock=FixedClock(111))
    engine.submit(
        PlaceLimit(
            instrument=Instrument("BTC-USD"),
            order_id=OrderId("o1"),
            side=Side.SELL,
            price=Price(200),
            qty=Qty(3),
            client_ts_ns=1,
        )
    )
    engine.submit(Cancel(instrument=Instrument("BTC-USD"), order_id=OrderId("o1"), client_ts_ns=2))

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
    engine = Engine(clock=FixedClock(1))
    engine.submit(
        PlaceLimit(
            instrument=Instrument("BTC-USD"),
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
            order_id=OrderId("o1"),
            side=Side.BUY,
            price=Price(101),
            qty=Qty(1),
            client_ts_ns=2,
        )
    )
    assert events[0].type == "OrderRejected"