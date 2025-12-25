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
    bids: dict[int, PriceLevel]
    asks: dict[int, PriceLevel]
    bid_prices: list[int]
    ask_prices: list[int]
    orders_by_id: dict[OrderId, (Side, int)]

    def __init__(self, instrument: Instrument) -> None:
        self.instrument = instrument
        self.bids = {}
        self.asks = {}
        self.bid_prices = []
        self.ask_prices = []
        self.orders_by_id = {}

    def best_bid(self) -> int | None:
        return self.bid_prices[-1] if self.bid_prices else None

    def best_ask(self) -> int | None:
        return self.ask_prices[0] if self.ask_prices else None

    def place_limit(self, order: RestingOrder) -> None:
        price = order.price.ticks
        if order.side == Side.BUY:
            if price not in self.bids:
                bisect.insort(self.bid_prices, price)
                self.bids[price] = PriceLevel(order.price)
            self.bids[price].add(order)
            self.orders_by_id[order.order_id] = (Side.BUY, price)
        elif order.side == Side.SELL:
            if price not in self.asks:
                bisect.insort(self.ask_prices, price)
                self.asks[price] = PriceLevel(order.price)
            self.asks[price].add(order)
            self.orders_by_id[order.order_id] = (Side.SELL, price)
    
    def cancel(self, order_id: OrderId) -> None:
        loc = self.orders_by_id.get(order_id, None)
        if loc is None:
            return
        side, price = loc
        if side == Side.BUY:
            self.bids[price].cancel(order_id)
            if not self.bids[price]:
                self.bid_prices.remove(price)
                del self.bids[price]
        elif side == Side.SELL:
            self.asks[price].cancel(order_id)
            if not self.asks[price]:
                self.ask_prices.remove(price)
                del self.asks[price]
        self.orders_by_id.pop(order_id, None)
