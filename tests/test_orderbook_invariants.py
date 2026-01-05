from __future__ import annotations

import pytest

from pyvenue.domain.types import Instrument, OrderId, Price, Qty, Side
from pyvenue.engine.orderbook import OrderBook, RestingOrder


def _o(
    oid: str, instrument: Instrument, side: Side, price: int, qty: int
) -> RestingOrder:
    return RestingOrder(
        order_id=OrderId(oid),
        instrument=instrument,
        side=side,
        price=Price(price),
        remaining=Qty(qty),
    )


def test_taker_does_not_rest_before_matching_when_fully_filled() -> None:
    """
    Guard against the bug where the incoming order is added to the book first
    and only then matched. A fully-filled taker must not remain cancelable/resting.
    """
    inst = Instrument("BTC-USD")
    book = OrderBook(inst)

    # Maker rests first
    assert book.place_limit(_o("a1", inst, Side.SELL, price=100, qty=5)) == []

    # Taker crosses and should fully fill immediately
    fills = book.place_limit(_o("b1", inst, Side.BUY, price=110, qty=5))
    assert [(f.maker_order_id, f.maker_price.ticks, f.qty.lots) for f in fills] == [
        (OrderId("a1"), 100, 5)
    ]

    # Maker removed (see (3) too)
    assert book.cancel(OrderId("a1")) is False

    # Taker must NOT be resting/cancelable if fully filled
    assert book.cancel(OrderId("b1")) is False

    # Book should be empty
    assert book.best_bid() is None
    assert book.best_ask() is None
    assert book.bid_prices == []
    assert book.ask_prices == []
    assert book.bids == {}
    assert book.asks == {}


def test_fully_filled_maker_is_removed_from_orders_by_id() -> None:
    """
    If a resting maker order is fully filled, it must also be removed from
    orders_by_id, otherwise later cancel() would incorrectly succeed.
    """
    inst = Instrument("BTC-USD")
    book = OrderBook(inst)

    book.place_limit(_o("a1", inst, Side.SELL, price=100, qty=5))
    book.place_limit(_o("b1", inst, Side.BUY, price=100, qty=5))

    # Both should be gone from the resting set
    assert book.cancel(OrderId("a1")) is False
    assert book.cancel(OrderId("b1")) is False


def test_cancel_must_not_create_missing_levels_when_out_of_sync() -> None:
    """
    cancel() must NOT call _ensure_level(): if orders_by_id points to a missing
    level, that is an out-of-sync invariant violation and should not create new levels.
    """
    inst = Instrument("BTC-USD")
    book = OrderBook(inst)

    # Corrupt state to simulate an invariant violation:
    # order appears in orders_by_id, but its level doesn't exist.
    book.orders_by_id[OrderId("ghost")] = (Side.BUY, 123)

    with pytest.raises(RuntimeError):
        book.cancel(OrderId("ghost"))

    # Must not have created a new level as a side effect
    assert 123 not in book.bids
    assert 123 not in book.asks
    assert 123 not in book.bid_prices
    assert 123 not in book.ask_prices


def test_cancel_respects_pricelevel_cancel_return_value() -> None:
    """
    If orders_by_id points to a level, but the order_id is missing inside that level,
    cancel() must fail (return False) and must not remove unrelated orders.
    """
    inst = Instrument("BTC-USD")
    book = OrderBook(inst)

    # Put a real resting order at 100
    assert book.place_limit(_o("real", inst, Side.BUY, price=100, qty=1)) == []

    # Corrupt: map a different order_id to the same level, but do NOT add it to the level
    book.orders_by_id[OrderId("fake")] = (Side.BUY, 100)

    # cancel must fail, not succeed
    assert book.cancel(OrderId("fake")) is False

    # The real order must still be cancelable (still resting)
    assert book.cancel(OrderId("real")) is True


def test_orderbook_rejects_orders_for_other_instruments() -> None:
    """
    OrderBook is single-instrument; placing an order for a different instrument
    should be rejected (raise ValueError).
    """
    book_inst = Instrument("BTC-USD")
    other_inst = Instrument("ETH-USD")
    book = OrderBook(book_inst)

    with pytest.raises(ValueError):
        book.place_limit(_o("x1", other_inst, Side.BUY, price=100, qty=1))
