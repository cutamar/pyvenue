from __future__ import annotations

import math

from pyvenue.domain.commands import PlaceLimit, PlaceMarket
from pyvenue.domain.types import (
    AccountId,
    Asset,
    FeeSchedule,
    Instrument,
    OrderId,
    Price,
    Qty,
    Side,
    TimeInForce,
)
from pyvenue.engine.engine import Engine

INSTR = Instrument("BTC-USD")
BASE = Asset("BTC")
QUOTE = Asset("USD")


def _engine_with_balances_and_fees(
    balances: dict[str, dict[str, int]],
    *,
    maker_bps: int,
    taker_bps: int,
    fee_account: str = "fee",
) -> Engine:
    """
    Test-driven API:
    - Engine accepts fee_schedule (or settable on state/venue)
    - EngineState supports credit/available/held like in your accounts milestone
    """
    e = Engine(
        instrument=INSTR,
        fee_schedule=FeeSchedule(
            maker_bps=maker_bps,
            taker_bps=taker_bps,
            fee_account=AccountId(fee_account),
            fee_asset=QUOTE,
        ),
    )

    for acct, assets in balances.items():
        for asset, amt in assets.items():
            e.state.credit(AccountId(acct), Asset(asset), amt)

    # ensure fee account exists
    if fee_account not in balances:
        e.state.credit(AccountId(fee_account), QUOTE, 0)

    return e


def _pl_limit(
    account: str, oid: str, side: Side, price: int, qty: int, ts: int
) -> PlaceLimit:
    return PlaceLimit(
        instrument=INSTR,
        account_id=AccountId(account),
        order_id=OrderId(oid),
        side=side,
        price=Price(price),
        qty=Qty(qty),
        client_ts_ns=ts,
        tif=TimeInForce.GTC,
        post_only=False,
    )


def _pl_mkt(account: str, oid: str, side: Side, qty: int, ts: int) -> PlaceMarket:
    return PlaceMarket(
        instrument=INSTR,
        account_id=AccountId(account),
        order_id=OrderId(oid),
        side=side,
        qty=Qty(qty),
        client_ts_ns=ts,
    )


def _avail(e: Engine, account: str, asset: Asset) -> int:
    return e.state.available(AccountId(account), asset)


def test_maker_and_taker_fees_charged_in_quote_and_paid_to_fee_account() -> None:
    """
    Maker = Bob (resting sell), Taker = Alice (market buy)
    Trade: 2 BTC @ 100 => notional = 200 USD

    maker_bps=10 => maker fee = ceil(200 * 0.001) = 1
    taker_bps=20 => taker fee = ceil(200 * 0.002) = 1 (since ints)
    fee account receives 2 USD total
    """
    e = _engine_with_balances_and_fees(
        {
            "alice": {"USD": 1_000, "BTC": 0},
            "bob": {"USD": 0, "BTC": 10},
        },
        maker_bps=10,
        taker_bps=20,
        fee_account="fee",
    )

    # Bob rests sell 2 BTC @ 100
    e.submit(_pl_limit("bob", "s1", Side.SELL, 100, 2, 1))

    # Alice market buys 2 BTC
    e.submit(_pl_mkt("alice", "mb1", Side.BUY, 2, 2))

    notional = 2 * 100
    maker_fee = math.ceil(notional * (10 / 10_000))
    taker_fee = math.ceil(notional * (20 / 10_000))
    total_fee = maker_fee + taker_fee

    # Alice pays notional + taker fee
    assert _avail(e, "alice", QUOTE) == 1_000 - notional - taker_fee
    assert _avail(e, "alice", BASE) == 2

    # Bob receives notional - maker fee
    assert _avail(e, "bob", QUOTE) == notional - maker_fee
    assert _avail(e, "bob", BASE) == 8  # sold 2 BTC

    # Fee account gets total
    assert _avail(e, "fee", QUOTE) == total_fee


def test_fee_rounding_is_ceiling_in_quote_units() -> None:
    """
    Ensure you never undercharge due to rounding.

    notional=101, taker_bps=10 => 0.101 => ceil -> 1
    """
    e = _engine_with_balances_and_fees(
        {
            "alice": {"USD": 1_000, "BTC": 0},
            "bob": {"USD": 0, "BTC": 1},
        },
        maker_bps=0,
        taker_bps=10,
        fee_account="fee",
    )

    e.submit(_pl_limit("bob", "s1", Side.SELL, 101, 1, 1))
    e.submit(_pl_mkt("alice", "mb1", Side.BUY, 1, 2))

    assert _avail(e, "fee", QUOTE) == 1


def test_no_fees_when_rates_are_zero() -> None:
    e = _engine_with_balances_and_fees(
        {
            "alice": {"USD": 1_000, "BTC": 0},
            "bob": {"USD": 0, "BTC": 1},
        },
        maker_bps=0,
        taker_bps=0,
        fee_account="fee",
    )

    e.submit(_pl_limit("bob", "s1", Side.SELL, 100, 1, 1))
    e.submit(_pl_mkt("alice", "mb1", Side.BUY, 1, 2))

    assert _avail(e, "fee", QUOTE) == 0
    assert _avail(e, "alice", QUOTE) == 900
    assert _avail(e, "bob", QUOTE) == 100


def test_fee_accounting_on_partial_fills_multiple_trades() -> None:
    """
    Two makers at different prices. Market buy sweeps two levels.
    Fees should be computed per-trade (sum of ceils), not ceil of total,
    to avoid mismatch (you choose; this test expects per-trade fee calc).
    """
    e = _engine_with_balances_and_fees(
        {
            "alice": {"USD": 10_000, "BTC": 0},
            "bob": {"USD": 0, "BTC": 10},
            "cara": {"USD": 0, "BTC": 10},
        },
        maker_bps=10,
        taker_bps=10,
        fee_account="fee",
    )

    # two makers:
    e.submit(_pl_limit("bob", "s1", Side.SELL, 100, 1, 1))  # notional 100
    e.submit(_pl_limit("cara", "s2", Side.SELL, 101, 1, 2))  # notional 101

    e.submit(_pl_mkt("alice", "mb1", Side.BUY, 2, 3))

    maker_fee_1 = math.ceil(100 * (10 / 10_000))
    maker_fee_2 = math.ceil(101 * (10 / 10_000))
    taker_fee_1 = math.ceil(100 * (10 / 10_000))
    taker_fee_2 = math.ceil(101 * (10 / 10_000))
    total_fee = maker_fee_1 + maker_fee_2 + taker_fee_1 + taker_fee_2

    assert _avail(e, "fee", QUOTE) == total_fee
