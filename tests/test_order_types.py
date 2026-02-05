from __future__ import annotations

from pyvenue.domain.commands import Cancel, PlaceLimit, PlaceMarket
from pyvenue.domain.events import Event, OrderExpired, OrderRejected, TradeOccurred
from pyvenue.domain.types import (
    AccountId,
    Instrument,
    OrderId,
    Price,
    Qty,
    Side,
    TimeInForce,
)
from pyvenue.engine.engine import Engine
from utils import NextMeta

INSTR = Instrument("BTC-USD")


def _pl_limit(
    oid: str,
    side: Side,
    price: int,
    qty: int,
    client_ts_ns: int,
    tif: TimeInForce = TimeInForce.GTC,
    post_only: bool = False,
) -> PlaceLimit:
    return PlaceLimit(
        instrument=INSTR,
        account_id=AccountId("alice"),
        order_id=OrderId(oid),
        side=side,
        price=Price(price),
        qty=Qty(qty),
        client_ts_ns=client_ts_ns,
        tif=tif,
        post_only=post_only,
    )


def _pl_mkt(
    oid: str,
    side: Side,
    qty: int,
    client_ts_ns: int,
) -> PlaceMarket:
    return PlaceMarket(
        instrument=INSTR,
        account_id=AccountId("alice"),
        order_id=OrderId(oid),
        side=side,
        qty=Qty(qty),
        client_ts_ns=client_ts_ns,
    )


def _cx(oid: str, client_ts_ns: int) -> Cancel:
    return Cancel(
        instrument=INSTR,
        account_id=AccountId("alice"),
        order_id=OrderId(oid),
        client_ts_ns=client_ts_ns,
    )


def _types(events: list[Event]) -> list[str]:
    return [e.__class__.__name__ for e in events]


def _trades(events: list[Event]) -> list[TradeOccurred]:
    return [e for e in events if isinstance(e, TradeOccurred)]


def _trade_prices(events: list[Event]) -> list[int]:
    return [t.price.ticks for t in _trades(events)]


def _trade_makers(events: list[Event]) -> list[OrderId]:
    return [t.maker_order_id for t in _trades(events)]


def test_market_buy_sweeps_multiple_levels_and_never_rests() -> None:
    """
    Setup asks:
      a1: 2 @ 100
      a2: 3 @ 105
    Market buy 4 => fills 2@100 and 2@105, leaves 1@105 resting.
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta())

    e.submit(_pl_limit("a1", Side.SELL, 100, 2, 1))
    e.submit(_pl_limit("a2", Side.SELL, 105, 3, 2))

    ev = e.submit(_pl_mkt("mb1", Side.BUY, 4, 3))

    # Must trade
    trades = _trades(ev)
    assert sum(t.qty.lots for t in trades) == 4
    assert 100 in _trade_prices(ev)
    assert 105 in _trade_prices(ev)
    assert OrderId("a1") in _trade_makers(ev)
    assert OrderId("a2") in _trade_makers(ev)

    # Remaining book: ask should still exist at 105
    assert e.book.best_ask() == 105
    assert e.book.best_bid() is None

    # Market taker should not be cancelable/resting
    # (it should never be present in the book)
    ev_c = e.submit(_cx("mb1", 4))
    assert isinstance(ev_c[0], OrderRejected)


def test_market_buy_on_empty_book_expires_or_rejects() -> None:
    """
    Market order on empty book cannot rest.
    We expect: accepted + expired OR rejected (choose one).
    This test is written for "accepted then expired".
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta())

    ev = e.submit(_pl_mkt("mb1", Side.BUY, 5, 1))
    names = _types(ev)

    # no trades
    assert "TradeOccurred" not in names

    # We expect an explicit terminal outcome for the taker.
    # This test expects OrderExpired to exist.
    assert "OrderExpired" in names
    exp = next(x for x in ev if isinstance(x, OrderExpired))
    assert exp.order_id == OrderId("mb1")


def test_ioc_limit_crosses_then_expires_remainder_and_does_not_rest() -> None:
    """
    Setup ask a1: 2@100
    IOC buy 5@100 => trades 2 and expires remainder 3. Should NOT rest bid.
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta())

    e.submit(_pl_limit("a1", Side.SELL, 100, 2, 1))

    ev = e.submit(_pl_limit("ioc1", Side.BUY, 100, 5, 2, tif=TimeInForce.IOC))
    names = _types(ev)

    assert "TradeOccurred" in names
    assert sum(t.qty.lots for t in _trades(ev)) == 2

    assert "OrderExpired" in names  # remainder expired
    exp = next(x for x in ev if isinstance(x, OrderExpired))
    assert exp.order_id == OrderId("ioc1")

    # a1 was fully filled -> asks empty
    assert e.book.best_ask() is None

    # IOC order must not rest as a bid
    assert e.book.best_bid() is None

    # Cancel IOC order should reject (not active/resting)
    ev_c = e.submit(_cx("ioc1", 3))
    assert isinstance(ev_c[0], OrderRejected)


def test_ioc_limit_that_does_not_cross_expires_entire_order_and_does_not_rest() -> None:
    """
    Setup ask a1: 1@100
    IOC buy 1@90 => no crossing => expires, does NOT rest.
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta())

    e.submit(_pl_limit("a1", Side.SELL, 100, 1, 1))
    assert e.book.best_ask() == 100

    ev = e.submit(_pl_limit("ioc1", Side.BUY, 90, 1, 2, tif=TimeInForce.IOC))
    names = _types(ev)

    assert "TradeOccurred" not in names
    assert "OrderExpired" in names

    # book unchanged
    assert e.book.best_ask() == 100
    assert e.book.best_bid() is None


def test_fok_limit_rejects_if_not_fully_fillable_and_does_not_mutate_book() -> None:
    """
    Setup asks total = 3 lots at prices <= 100
      a1: 1@100
      a2: 2@100
    FOK buy 4@100 => not fillable => reject, no trades, book unchanged.
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta())

    e.submit(_pl_limit("a1", Side.SELL, 100, 1, 1))
    e.submit(_pl_limit("a2", Side.SELL, 100, 2, 2))
    assert e.book.best_ask() == 100

    ev = e.submit(_pl_limit("fok1", Side.BUY, 100, 4, 3, tif=TimeInForce.FOK))
    assert len(ev) == 1
    assert isinstance(ev[0], OrderRejected)
    assert "fok" in ev[0].reason.lower() or "fill" in ev[0].reason.lower()

    # Book unchanged (no maker consumed)
    assert e.book.best_ask() == 100

    # Makers still cancelable (proves no mutation)
    assert e.submit(_cx("a1", 4))[0].__class__.__name__ != "OrderRejected"
    # after canceling a1, ask still exists due to a2
    assert e.book.best_ask() == 100


def test_fok_limit_fills_fully_and_does_not_rest() -> None:
    """
    Setup asks total = 5 lots at <= 100
      a1: 2@100
      a2: 3@100
    FOK buy 5@100 => fully fills, no rest for taker.
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta())

    e.submit(_pl_limit("a1", Side.SELL, 100, 2, 1))
    e.submit(_pl_limit("a2", Side.SELL, 100, 3, 2))

    ev = e.submit(_pl_limit("fok1", Side.BUY, 100, 5, 3, tif=TimeInForce.FOK))
    trades = _trades(ev)
    assert sum(t.qty.lots for t in trades) == 5
    assert e.book.best_ask() is None
    assert e.book.best_bid() is None  # fok taker never rests


def test_post_only_rejects_if_it_would_cross_and_does_not_trade() -> None:
    """
    Setup ask a1: 1@100
    Post-only buy 1@100 would cross => must reject (or cancel), and no trade occurs.
    This test expects REJECT.
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta())
    e.submit(_pl_limit("a1", Side.SELL, 100, 1, 1))

    ev = e.submit(
        _pl_limit("po1", Side.BUY, 100, 1, 2, tif=TimeInForce.GTC, post_only=True)
    )
    assert len(ev) == 1
    assert isinstance(ev[0], OrderRejected)
    assert "post" in ev[0].reason.lower() or "maker" in ev[0].reason.lower()

    # maker not consumed => ask still there
    assert e.book.best_ask() == 100


def test_post_only_rest_if_it_does_not_cross() -> None:
    """
    Setup ask a1: 1@100
    Post-only buy 1@90 does not cross => should rest as bid at 90.
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta())
    e.submit(_pl_limit("a1", Side.SELL, 100, 1, 1))

    ev = e.submit(
        _pl_limit("po1", Side.BUY, 90, 1, 2, tif=TimeInForce.GTC, post_only=True)
    )
    names = _types(ev)

    # no trades
    assert "TradeOccurred" not in names

    # bid should rest
    assert e.book.best_bid() == 90
    assert e.book.best_ask() == 100


def test_market_sell_partially_fills_then_expires_remainder_and_consumes_bids() -> None:
    """
    Setup bids:
      b1: 2 @ 100
    Market sell 5 => fills 2@100, expires remaining 3, leaves book empty.
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta())

    e.submit(_pl_limit("b1", Side.BUY, 100, 2, 1))

    ev = e.submit(_pl_mkt("ms1", Side.SELL, 5, 2))
    names = _types(ev)

    assert "TradeOccurred" in names
    assert sum(t.qty.lots for t in _trades(ev)) == 2
    assert "OrderExpired" in names

    # bids consumed
    assert e.book.best_bid() is None
    assert e.book.best_ask() is None


def test_ioc_limit_partial_fill_expires_only_the_remainder() -> None:
    """
    Setup asks:
      a1: 2 @ 100
      a2: 10 @ 110
    IOC buy 5 @ 100 => fills 2@100, remainder 3 expires (does not rest)
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta())

    e.submit(_pl_limit("a1", Side.SELL, 100, 2, 1))
    e.submit(_pl_limit("a2", Side.SELL, 110, 10, 2))

    ev = e.submit(_pl_limit("ioc1", Side.BUY, 100, 5, 3, tif=TimeInForce.IOC))
    names = _types(ev)

    assert "TradeOccurred" in names
    assert sum(t.qty.lots for t in _trades(ev)) == 2
    assert "OrderExpired" in names

    # ask a2 untouched, best ask now 110
    assert e.book.best_ask() == 110
    assert e.book.best_bid() is None


def test_fok_limit_not_fillable_due_to_price_constraint_rejects_without_mutation() -> (
    None
):
    """
    Setup asks:
      a1: 10 @ 101
    FOK buy 5 @ 100 cannot cross (100 < 101) => reject, no trades, book unchanged.
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta())

    e.submit(_pl_limit("a1", Side.SELL, 101, 10, 1))
    assert e.book.best_ask() == 101

    ev = e.submit(_pl_limit("fok1", Side.BUY, 100, 5, 2, tif=TimeInForce.FOK))
    assert len(ev) == 1
    assert isinstance(ev[0], OrderRejected)
    assert "fok" in ev[0].reason.lower() or "fill" in ev[0].reason.lower()

    # book unchanged
    assert e.book.best_ask() == 101
    assert e.book.best_bid() is None


def test_post_only_rejects_if_crossing_even_if_crossing_would_take_multiple_levels() -> (
    None
):
    """
    Setup asks:
      a1: 1 @ 100
      a2: 1 @ 101
    Post-only buy 2 @ 200 would cross => reject, no trades, book unchanged.
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta())

    e.submit(_pl_limit("a1", Side.SELL, 100, 1, 1))
    e.submit(_pl_limit("a2", Side.SELL, 101, 1, 2))

    ev = e.submit(_pl_limit("po1", Side.BUY, 200, 2, 3, post_only=True))
    assert len(ev) == 1
    assert isinstance(ev[0], OrderRejected)

    # no trades happened, makers remain
    assert e.book.best_ask() == 100


def test_duplicate_order_id_rejected_for_market_orders() -> None:
    e = Engine(instrument=INSTR, next_meta=NextMeta())

    ev1 = e.submit(_pl_mkt("m1", Side.BUY, 1, 1))
    # depending on your semantics, might expire immediately; still should create a record
    assert "OrderRejected" not in _types(ev1)

    ev2 = e.submit(_pl_mkt("m1", Side.BUY, 1, 2))
    assert len(ev2) == 1
    assert isinstance(ev2[0], OrderRejected)
    assert "duplicate" in ev2[0].reason.lower()


def test_qty_must_be_positive_for_market_and_limit() -> None:
    e = Engine(instrument=INSTR, next_meta=NextMeta())

    ev1 = e.submit(_pl_mkt("m1", Side.BUY, 0, 1))
    assert len(ev1) == 1
    assert isinstance(ev1[0], OrderRejected)

    ev2 = e.submit(_pl_limit("l1", Side.BUY, 100, 0, 2))
    assert len(ev2) == 1
    assert isinstance(ev2[0], OrderRejected)
