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

    def __init__(self, price: Price) -> None:
        self.price = price
        self.orders = OrderedDict()

    def __len__(self) -> int:
        return len(self.orders)

    def add(self, order: RestingOrder) -> None:
        self.orders[order.order_id] = order

    def cancel(self, order_id: OrderId) -> None:
        del self.orders[order_id]

    def peek_oldest(self) -> RestingOrder | None:
        return next(iter(self.orders.values()), None)

    def pop_oldest(self) -> RestingOrder | None:
        return self.orders.popitem(last=False)[0]


class OrderBook:
    def __init__(self) -> None:
        pass
