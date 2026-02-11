from __future__ import annotations

from pyvenue.domain.commands import Cancel, PlaceLimit
from pyvenue.domain.events import Event
from pyvenue.domain.types import AccountId, Instrument, OrderId, Price, Qty, Side
from pyvenue.engine.engine import Engine
from pyvenue.engine.state import OrderRecord, OrderStatus
from utils import NextMeta, engine_with_balances


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


def _remaining_lots(record: OrderRecord) -> int:
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
    e = engine_with_balances(
        inst,
        {"alice": {"USD": 999999, "BTC": 999999}},
    )

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
    e = engine_with_balances(
        inst,
        {"alice": {"USD": 999999, "BTC": 999999}},
    )

    all_events = []
    all_events.extend(e.submit(_pl(inst, "m1", Side.SELL, 100, 5, 1)))
    all_events.extend(e.submit(_pl(inst, "t1", Side.BUY, 200, 2, 1)))
    all_events.extend(e.submit(_pl(inst, "t2", Side.BUY, 200, 3, 1)))

    snap1 = _snapshot(e)

    # You implement this in milestone 3:
    # either a @classmethod or a standalone helper; test expects a classmethod:
    r = Engine.replay(instrument=inst, events=all_events, next_meta=NextMeta())

    snap2 = _snapshot(r)
    assert snap2 == snap1


def test_replay_rebuilds_resting_book_levels_and_orders() -> None:
    """
    Replay should rebuild the book so that:
    - best bid/ask reflect the resting orders
    - cancel works for resting orders after replay
    """
    inst = Instrument("BTC-USD")
    e = engine_with_balances(
        inst,
        {"alice": {"USD": 999999, "BTC": 999999}},
    )

    all_events: list[Event] = []
    # Resting ask m1: 5@100
    all_events.extend(e.submit(_pl(inst, "m1", Side.SELL, 100, 5, 1)))

    # Partial fill m1 with taker buys: buy 2@200
    all_events.extend(e.submit(_pl(inst, "t1", Side.BUY, 200, 2, 1)))

    # After these, m1 should still be resting (remaining 3)
    assert e.book.best_ask() == 100

    # Rebuild from events
    r = Engine.replay(
        instrument=inst, events=all_events, next_meta=NextMeta(), rebuild_book=True
    )

    # Book should reflect the remaining resting ask
    assert r.book.best_ask() == 100
    assert r.book.best_bid() is None

    # And cancel should work post-replay (proves it's actually in the book)
    ev_c = r.submit(_cx(inst, "m1", 1))
    assert "OrderCanceled" in _types(ev_c)

    # Book becomes empty on ask side
    assert r.book.best_ask() is None


def test_replay_book_allows_matching_after_replay() -> None:
    """
    After replay+rebuild_book, the reconstructed book should be usable for matching.
    Place a new crossing order and ensure a trade occurs against the resting maker.
    """
    inst = Instrument("BTC-USD")
    e = engine_with_balances(
        inst,
        {"alice": {"USD": 999999, "BTC": 999999}},
    )

    all_events: list[Event] = []
    all_events.extend(e.submit(_pl(inst, "m1", Side.SELL, 100, 3, 1)))  # maker rests

    # Replay into new engine with rebuilt book
    r = Engine.replay(
        instrument=inst, events=all_events, next_meta=NextMeta(), rebuild_book=True
    )

    # Now cross it with a taker buy
    ev = r.submit(_pl(inst, "t1", Side.BUY, 200, 2, 1))
    names = _types(ev)

    assert "TradeOccurred" in names

    # Maker should still be resting with remaining 1, so best ask remains 100
    assert r.book.best_ask() == 100

    # And cancel should still work
    ev_c = r.submit(_cx(inst, "m1", 1))
    assert "OrderCanceled" in _types(ev_c)
    assert r.book.best_ask() is None


def test_replay_book_applies_trade_occurred_to_maker_remaining() -> None:
    """
    Replay+rebuild_book must apply TradeOccurred to the book:
    maker remaining decreases (and the order stays resting if remaining > 0).
    """
    inst = Instrument("BTC-USD")
    e = engine_with_balances(
        inst,
        {"alice": {"USD": 999999, "BTC": 999999}},
    )

    all_events: list[Event] = []
    # maker rests 5@100
    all_events.extend(e.submit(_pl(inst, "m1", Side.SELL, 100, 5, 1)))
    # taker buys 2, so maker remaining should become 3
    all_events.extend(e.submit(_pl(inst, "t1", Side.BUY, 200, 2, 2)))

    r = Engine.replay(
        instrument=inst, events=all_events, next_meta=NextMeta(), rebuild_book=True
    )

    # maker should still be resting at 100
    assert r.book.best_ask() == 100

    # and cancel should succeed (proves it's actually there)
    assert r.book.cancel(OrderId("m1")) is True


def test_replay_book_trade_fully_filled_maker_is_not_cancelable() -> None:
    """
    Replay+rebuild_book must apply TradeOccurred to the book:
    fully filled maker order is removed and is no longer cancelable.
    """
    inst = Instrument("BTC-USD")
    e = engine_with_balances(
        inst,
        {"alice": {"USD": 999999, "BTC": 999999}},
    )

    all_events: list[Event] = []
    all_events.extend(e.submit(_pl(inst, "m1", Side.SELL, 100, 2, 1)))
    all_events.extend(e.submit(_pl(inst, "t1", Side.BUY, 200, 2, 2)))  # fully fills m1

    r = Engine.replay(
        instrument=inst, events=all_events, next_meta=NextMeta(), rebuild_book=True
    )

    # If TradeOccurred is applied to the reconstructed book, m1 is gone => cancel must fail
    assert r.book.cancel(OrderId("m1")) is False
    assert r.book.best_ask() is None


def test_replay_book_applies_order_canceled_removes_resting_order() -> None:
    """
    Replay+rebuild_book must apply OrderCanceled to the book:
    canceled resting order is removed and is no longer cancelable.
    """
    inst = Instrument("BTC-USD")
    e = engine_with_balances(
        inst,
        {"alice": {"USD": 999999, "BTC": 999999}},
    )

    all_events: list[Event] = []
    all_events.extend(e.submit(_pl(inst, "m1", Side.SELL, 100, 5, 1)))
    all_events.extend(e.submit(_cx(inst, "m1", 2)))

    r = Engine.replay(
        instrument=inst, events=all_events, next_meta=NextMeta(), rebuild_book=True
    )

    # canceled order must not be present
    assert r.book.best_ask() is None
    assert r.book.cancel(OrderId("m1")) is False
