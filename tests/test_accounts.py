from __future__ import annotations

from pyvenue.domain.commands import Cancel, PlaceLimit, PlaceMarket
from pyvenue.domain.events import Event, OrderRejected, TradeOccurred
from pyvenue.domain.types import (
    AccountId,
    Asset,
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
BASE = Asset("BTC")
QUOTE = Asset("USD")


def _engine_with_balances(balances: dict[str, dict[str, int]]) -> Engine:
    """
    Test-driven API: You implement this by either:
      - letting Engine accept initial_balances=..., OR
      - providing a helper/factory in tests that seeds EngineState/ledger.

    balances example:
      {"alice": {"USD": 10_000, "BTC": 0}, "bob": {"USD": 0, "BTC": 5}}
    """
    e = Engine(instrument=INSTR, next_meta=NextMeta())

    # Test-driven expectation: you provide a clean API to seed balances.
    # Recommended: e.state.ledger.credit(account, asset, amount)
    for acct, assets in balances.items():
        for asset, amt in assets.items():
            e.state.credit(AccountId(acct), Asset(asset), amt)

    return e


def _pl_limit(
    account: str,
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
        account_id=AccountId(account),
        order_id=OrderId(oid),
        side=side,
        price=Price(price),
        qty=Qty(qty),
        client_ts_ns=client_ts_ns,
        tif=tif,
        post_only=post_only,
    )


def _pl_mkt(
    account: str, oid: str, side: Side, qty: int, client_ts_ns: int
) -> PlaceMarket:
    return PlaceMarket(
        instrument=INSTR,
        account_id=AccountId(account),
        order_id=OrderId(oid),
        side=side,
        qty=Qty(qty),
        client_ts_ns=client_ts_ns,
    )


def _cx(account: str, oid: str, client_ts_ns: int) -> Cancel:
    return Cancel(
        instrument=INSTR,
        account_id=AccountId(account),
        order_id=OrderId(oid),
        client_ts_ns=client_ts_ns,
    )


def _types(events: list[Event]) -> list[str]:
    return [e.__class__.__name__ for e in events]


def _trades(events: list[Event]) -> list[TradeOccurred]:
    return [e for e in events if isinstance(e, TradeOccurred)]


def _avail(e: Engine, account: str, asset: Asset) -> int:
    """
    Test-driven API: implement these accessors on state (recommended).
      e.state.available(AccountId, Asset) -> int
    """
    return e.state.available(AccountId(account), asset)


def _held(e: Engine, account: str, asset: Asset) -> int:
    """
    Test-driven API:
      e.state.held(AccountId, Asset) -> int
    """
    return e.state.held(AccountId(account), asset)


def test_buy_limit_rejected_if_insufficient_quote_balance() -> None:
    """
    Alice has 50 USD. Wants to buy 1 BTC @ 100 => needs 100 USD reserved => reject.
    """
    e = _engine_with_balances({"alice": {"USD": 50, "BTC": 0}})

    ev = e.submit(_pl_limit("alice", "b1", Side.BUY, price=100, qty=1, client_ts_ns=1))
    assert len(ev) == 1
    assert isinstance(ev[0], OrderRejected)
    assert "insufficient" in ev[0].reason.lower()


def test_sell_limit_rejected_if_insufficient_base_balance() -> None:
    """
    Bob has 0 BTC. Wants to sell 1 BTC => reject.
    """
    e = _engine_with_balances({"bob": {"USD": 0, "BTC": 0}})

    ev = e.submit(_pl_limit("bob", "s1", Side.SELL, price=100, qty=1, client_ts_ns=1))
    assert len(ev) == 1
    assert isinstance(ev[0], OrderRejected)
    assert "insufficient" in ev[0].reason.lower()


def test_gtc_resting_buy_reserves_quote_and_cancel_releases() -> None:
    """
    Alice places GTC buy 2 BTC @ 100 with no asks => it rests.
    Should reserve 200 USD until canceled.
    """
    e = _engine_with_balances({"alice": {"USD": 1_000, "BTC": 0}})

    ev = e.submit(_pl_limit("alice", "b1", Side.BUY, price=100, qty=2, client_ts_ns=1))
    assert "OrderAccepted" in _types(ev)

    assert _avail(e, "alice", QUOTE) == 800
    assert _held(e, "alice", QUOTE) == 200

    ev_c = e.submit(_cx("alice", "b1", client_ts_ns=2))
    assert "OrderCanceled" in _types(ev_c)

    assert _avail(e, "alice", QUOTE) == 1_000
    assert _held(e, "alice", QUOTE) == 0


def test_gtc_resting_sell_reserves_base_and_cancel_releases() -> None:
    """
    Bob places GTC sell 3 BTC @ 200 with no bids => it rests.
    Should reserve 3 BTC until canceled.
    """
    e = _engine_with_balances({"bob": {"USD": 0, "BTC": 10}})

    e.submit(_pl_limit("bob", "s1", Side.SELL, price=200, qty=3, client_ts_ns=1))
    assert _avail(e, "bob", BASE) == 7
    assert _held(e, "bob", BASE) == 3

    e.submit(_cx("bob", "s1", client_ts_ns=2))
    assert _avail(e, "bob", BASE) == 10
    assert _held(e, "bob", BASE) == 0


def test_trade_settles_balances_for_taker_buy_against_resting_sell() -> None:
    """
    Bob rests: sell 2 BTC @ 100
      - reserves 2 BTC
    Alice market buys 2 BTC
      - spends 200 USD
      - receives 2 BTC
    Bob receives 200 USD, and reserved BTC is consumed.
    """
    e = _engine_with_balances(
        {
            "alice": {"USD": 1_000, "BTC": 0},
            "bob": {"USD": 0, "BTC": 10},
        }
    )

    # maker rests
    e.submit(_pl_limit("bob", "s1", Side.SELL, price=100, qty=2, client_ts_ns=1))
    assert _avail(e, "bob", BASE) == 8
    assert _held(e, "bob", BASE) == 2

    ev = e.submit(_pl_mkt("alice", "mb1", Side.BUY, qty=2, client_ts_ns=2))
    assert sum(t.qty.lots for t in _trades(ev)) == 2

    # Alice paid 200 USD, got 2 BTC
    assert _avail(e, "alice", QUOTE) == 800
    assert _avail(e, "alice", BASE) == 2
    assert _held(e, "alice", QUOTE) == 0
    assert _held(e, "alice", BASE) == 0

    # Bob delivered 2 BTC and got 200 USD; hold consumed
    assert _avail(e, "bob", BASE) == 8
    assert _held(e, "bob", BASE) == 0
    assert _avail(e, "bob", QUOTE) == 200


def test_ioc_buy_reserves_nothing_and_expires_remainder() -> None:
    """
    IOC should not leave any holds behind.
    Setup: Bob sells 1 BTC @ 100
    Alice IOC buys 3 @ 100 => fills 1, expires 2, no resting, no hold remains.
    """
    e = _engine_with_balances(
        {"alice": {"USD": 1_000, "BTC": 0}, "bob": {"USD": 0, "BTC": 5}}
    )

    e.submit(_pl_limit("bob", "s1", Side.SELL, 100, 1, 1))

    ev = e.submit(_pl_limit("alice", "ioc1", Side.BUY, 100, 3, 2, tif=TimeInForce.IOC))
    assert sum(t.qty.lots for t in _trades(ev)) == 1
    assert "OrderExpired" in _types(ev)

    # Alice spent 100 USD, received 1 BTC, no remaining hold
    assert _avail(e, "alice", QUOTE) == 900
    assert _avail(e, "alice", BASE) == 1
    assert _held(e, "alice", QUOTE) == 0


def test_fok_buy_rejected_and_does_not_reserve_when_not_fillable() -> None:
    """
    FOK buy must be all-or-nothing and must not leave holds if rejected.

    Setup: only 1 BTC available at <= 100
    Alice FOK buys 2 @ 100 => reject, balances unchanged.
    """
    e = _engine_with_balances(
        {"alice": {"USD": 1_000, "BTC": 0}, "bob": {"USD": 0, "BTC": 1}}
    )

    e.submit(_pl_limit("bob", "s1", Side.SELL, 100, 1, 1))

    ev = e.submit(_pl_limit("alice", "fok1", Side.BUY, 100, 2, 2, tif=TimeInForce.FOK))
    assert len(ev) == 1
    assert isinstance(ev[0], OrderRejected)

    assert _avail(e, "alice", QUOTE) == 1_000
    assert _held(e, "alice", QUOTE) == 0
