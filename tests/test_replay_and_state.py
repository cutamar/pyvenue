from __future__ import annotations

from pyvenue.domain.commands import PlaceLimit
from pyvenue.domain.types import Instrument, OrderId, Price, Qty, Side
from pyvenue.engine.engine import Engine
from pyvenue.engine.state import OrderStatus


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


def _remaining_lots(record: object) -> int:
    """
    Adjust this helper if your OrderRecord uses a different field name.
    Expected: record.remaining is a Qty.
    """
    rem = record.remaining
    return rem.lots


def _snapshot(engine: Engine) -> dict[OrderId, tuple[OrderStatus, int]]:
    out: dict[OrderId, tuple[OrderStatus, int]] = {}
    for oid, rec in engine.state.orders.items():
        out[oid] = (rec.status, _remaining_lots(rec))
    return out


def test_state_updates_remaining_and_status_for_maker_and_taker() -> None:
    inst = Instrument("BTC-USD")
    e = Engine(instrument=inst)

    # Maker rests: sell 5 @ 100
    e.submit(_pl(inst, "m1", Side.SELL, 100, 5, 1))
    m1 = e.state.orders[OrderId("m1")]
    assert m1.status == OrderStatus.ACTIVE
    assert _remaining_lots(m1) == 5

    # Taker crosses: buy 2 @ 200 -> fills 2 lots against m1
    e.submit(_pl(inst, "t1", Side.BUY, 200, 2, 1))

    m1 = e.state.orders[OrderId("m1")]
    t1 = e.state.orders[OrderId("t1")]

    assert _remaining_lots(m1) == 3
    assert m1.status == OrderStatus.ACTIVE

    # Taker should be fully filled (since it doesn't rest)
    assert _remaining_lots(t1) == 0
    assert t1.status == OrderStatus.FILLED

    # Another taker fills remaining 3 -> maker becomes FILLED
    e.submit(_pl(inst, "t2", Side.BUY, 200, 3, 1))
    m1 = e.state.orders[OrderId("m1")]
    assert _remaining_lots(m1) == 0
    assert m1.status == OrderStatus.FILLED


def test_replay_reconstructs_same_state() -> None:
    inst = Instrument("BTC-USD")
    e = Engine(instrument=inst)

    all_events = []
    all_events.extend(e.submit(_pl(inst, "m1", Side.SELL, 100, 5, 1)))
    all_events.extend(e.submit(_pl(inst, "t1", Side.BUY, 200, 2, 1)))
    all_events.extend(e.submit(_pl(inst, "t2", Side.BUY, 200, 3, 1)))

    snap1 = _snapshot(e)

    # You implement this in milestone 3:
    # either a @classmethod or a standalone helper; test expects a classmethod:
    r = Engine.replay(instrument=inst, events=all_events)

    snap2 = _snapshot(r)
    assert snap2 == snap1
