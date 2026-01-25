from __future__ import annotations

from dataclasses import dataclass

from pyvenue.domain.types import Instrument, OrderId, Price, Qty, Side


@dataclass(frozen=True, slots=True, kw_only=True)
class PlaceLimit:
    instrument: Instrument
    order_id: OrderId
    side: Side
    price: Price
    qty: Qty
    client_ts_ns: int


@dataclass(frozen=True, slots=True, kw_only=True)
class Cancel:
    instrument: Instrument
    order_id: OrderId
    client_ts_ns: int


@dataclass(frozen=True, slots=True, kw_only=True)
class PlaceMarket:
    instrument: Instrument
    order_id: OrderId
    side: Side
    qty: Qty
    client_ts_ns: int


Command = PlaceLimit | Cancel | PlaceMarket
