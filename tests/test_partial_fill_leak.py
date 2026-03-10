from __future__ import annotations

from pyvenue.domain.commands import PlaceLimit
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
from utils import engine_with_balances

INSTR = Instrument("BTC-USD")


def _pl_limit(
    oid: str,
    side: Side,
    price: int,
    qty: int,
    client_ts_ns: int,
    tif: TimeInForce = TimeInForce.GTC,
    post_only: bool = False,
    account_id: str = "alice",
) -> PlaceLimit:
    return PlaceLimit(
        instrument=INSTR,
        account_id=AccountId(account_id),
        order_id=OrderId(oid),
        side=side,
        price=Price(price),
        qty=Qty(qty),
        client_ts_ns=client_ts_ns,
        tif=tif,
        post_only=post_only,
    )


def test_partial_fill_reservation_leak():
    # Setup engine with limited balances
    e = engine_with_balances(
        INSTR,
        {
            "alice": {"USD": 1000, "BTC": 100},
            "bob": {"USD": 1000, "BTC": 100},
        },
    )

    # Alice rests a SELL for 5 BTC at 100 USD
    e.submit(_pl_limit("a1", Side.SELL, 100, 5, 1, account_id="alice"))

    # Bob has 1000 USD available.
    # Bob submits a BUY for 6 BTC at 100 USD (needs 600 USD).
    # Matches 5 BTC @ 100 USD (cost 500 USD).
    # Then remaining 1 BTC @ 100 USD (cost 100 USD) should be reserved.
    # Total available should go from 1000 -> 1000 - 500 - 100 = 400.
    # Total held should be 100 USD.
    e.submit(_pl_limit("b1", Side.BUY, 100, 6, 2, account_id="bob"))

    bob_avail = e.state.available(AccountId("bob"), Asset("USD"))
    bob_held = e.state.accounts_held.get((AccountId("bob"), Asset("USD")), 0)

    print(f"Bob Available: {bob_avail}")
    print(f"Bob Held: {bob_held}")
    assert bob_held == 100, f"Expected 100 held, got {bob_held}"
    assert bob_avail == 400, f"Expected 400 available, got {bob_avail}"
