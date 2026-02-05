from __future__ import annotations

from dataclasses import dataclass

from pyvenue.domain.types import (
    AccountId,
    Instrument,
    OrderId,
    Price,
    Qty,
    Side,
    TimeInForce,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class PlaceLimit:
    instrument: Instrument
    account_id: AccountId
    order_id: OrderId
    side: Side
    price: Price
    qty: Qty
    client_ts_ns: int
    tif: TimeInForce = TimeInForce.GTC
    post_only: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class Cancel:
    instrument: Instrument
    account_id: AccountId
    order_id: OrderId
    client_ts_ns: int


@dataclass(frozen=True, slots=True, kw_only=True)
class PlaceMarket:
    instrument: Instrument
    account_id: AccountId
    order_id: OrderId
    side: Side
    qty: Qty
    client_ts_ns: int


Command = PlaceLimit | Cancel | PlaceMarket
