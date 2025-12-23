from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass

from pyvenue.domain.types import Instrument, OrderId, Price, Qty, Side


@dataclass(slots=True)
class RestingOrder:
    order_id: OrderId
    instrument: Instrument
    side: Side
    price: Price
    remaining: Qty


@dataclass(frozen=True, slots=True)
class Fill:
    maker_order_id: OrderId
    maker_price: Price
    qty: Qty


@dataclass(slots=True)
class PriceLevel:
    price: Price
    orders: OrderedDict[OrderId, RestingOrder]


class OrderBook:
    def __init__(self) -> None:
        pass
