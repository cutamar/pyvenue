from __future__ import annotations

import bisect
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
    """A price level in the order book.
    Adding a duplicate order_id will overwrite the existing order."""

    price: Price
    orders: OrderedDict[OrderId, RestingOrder]

    def __init__(self, price: Price) -> None:
        self.price = price
        self.orders = OrderedDict()

    def __len__(self) -> int:
        return len(self.orders)

    def __bool__(self) -> bool:
        return bool(self.orders)

    def add(self, order: RestingOrder) -> None:
        self.orders[order.order_id] = order

    def cancel(self, order_id: OrderId) -> bool:
        return self.orders.pop(order_id, None) is not None

    def peek_oldest(self) -> RestingOrder | None:
        return next(iter(self.orders.values()), None)

    def pop_oldest(self) -> RestingOrder:
        _, order = self.orders.popitem(last=False)
        return order


class OrderBook:
    """An order book for a single instrument."""

    instrument: Instrument
    bids: OrderedDict[int, PriceLevel]
    asks: OrderedDict[int, PriceLevel]
    bid_prices: list[int]
    ask_prices: list[int]

    def __init__(self, instrument: Instrument) -> None:
        self.instrument = instrument
        self.bids = OrderedDict()
        self.asks = OrderedDict()
        self.bid_prices = []
        self.ask_prices = []

    def best_bid(self) -> int | None:
        return self.bid_prices[-1] if self.bid_prices else None

    def best_ask(self) -> int | None:
        return self.ask_prices[0] if self.ask_prices else None

    def place_limit(self, order: RestingOrder) -> None:
        price = order.price.ticks
        if order.side == Side.BUY:
            if price not in self.bids:
                self.bids[price] = PriceLevel(order.price)
            self.bids[price].add(order)
            bisect.insort(self.bid_prices, price)
        else:
            if price not in self.asks:
                self.asks[price] = PriceLevel(order.price)
            self.asks[price].add(order)
            bisect.insort(self.ask_prices, price)
