from __future__ import annotations

from pyvenue.domain.commands import Cancel, PlaceLimit
from pyvenue.domain.events import Event, TopOfBookChanged
from pyvenue.domain.types import AccountId, Instrument, OrderId, Price, Qty, Side
from utils import engine_with_balances


def _pl(
    inst: Instrument,
    oid: str,
    side: Side,
    price_ticks: int,
    qty_lots: int,
    client_ts_ns: int,
) -> PlaceLimit:
    return PlaceLimit(
        instrument=inst,
        account_id=AccountId("alice"),
        order_id=OrderId(oid),
        side=side,
        price=Price(price_ticks),
        qty=Qty(qty_lots),
        client_ts_ns=client_ts_ns,
    )


def _cx(inst: Instrument, oid: str, client_ts_ns: int) -> Cancel:
    return Cancel(
        instrument=inst,
        account_id=AccountId("alice"),
        order_id=OrderId(oid),
        client_ts_ns=client_ts_ns,
    )


def _bbo(events: list[Event]) -> list[TopOfBookChanged]:
    return [e for e in events if isinstance(e, TopOfBookChanged)]


def test_bbo_emitted_on_first_bid() -> None:
    inst = Instrument("BTC-USD")
    e = engine_with_balances(
        inst,
        {"alice": {"USD": 999999, "BTC": 999999}},
    )

    events = e.submit(_pl(inst, "b1", Side.BUY, 100, 1, 1))
    bbo = _bbo(events)

    assert len(bbo) == 1
    assert bbo[0].instrument == inst
    assert bbo[0].best_bid_ticks == 100
    assert bbo[0].best_ask_ticks is None


def test_bbo_emitted_on_first_ask() -> None:
    inst = Instrument("BTC-USD")
    e = engine_with_balances(
        inst,
        {"alice": {"USD": 999999, "BTC": 999999}},
    )

    events = e.submit(_pl(inst, "a1", Side.SELL, 101, 1, 1))
    bbo = _bbo(events)

    assert len(bbo) == 1
    assert bbo[0].best_bid_ticks is None
    assert bbo[0].best_ask_ticks == 101


def test_bbo_updates_when_best_ask_level_removed_by_trade() -> None:
    inst = Instrument("BTC-USD")
    e = engine_with_balances(
        inst,
        {"alice": {"USD": 999999, "BTC": 999999}},
    )

    # Rest two asks: best ask is 100, next is 105
    e.submit(_pl(inst, "a1", Side.SELL, 100, 1, 1))
    e.submit(_pl(inst, "a2", Side.SELL, 105, 1, 1))

    # Crossing buy consumes the 100 level entirely -> best ask becomes 105
    events = e.submit(_pl(inst, "b1", Side.BUY, 200, 1, 1))
    bbo = _bbo(events)

    assert len(bbo) == 1
    assert bbo[0].best_ask_ticks == 105


def test_bbo_updates_when_best_bid_level_removed_by_cancel() -> None:
    inst = Instrument("BTC-USD")
    e = engine_with_balances(
        inst,
        {"alice": {"USD": 999999, "BTC": 999999}},
    )

    # Rest two bids: best bid is 110
    e.submit(_pl(inst, "b1", Side.BUY, 100, 1, 1))
    e.submit(_pl(inst, "b2", Side.BUY, 110, 1, 1))

    events = e.submit(_cx(inst, "b2", 1))
    bbo = _bbo(events)

    assert len(bbo) == 1
    assert bbo[0].best_bid_ticks == 100


def test_bbo_not_emitted_when_top_of_book_unchanged() -> None:
    inst = Instrument("BTC-USD")
    e = engine_with_balances(
        inst,
        {"alice": {"USD": 999999, "BTC": 999999}},
    )

    # Establish top-of-book with best bid 100
    e.submit(_pl(inst, "b1", Side.BUY, 100, 1, 1))

    # Add a worse bid; top-of-book should not change
    events = e.submit(_pl(inst, "b2", Side.BUY, 90, 1, 1))
    bbo = _bbo(events)

    assert bbo == []
