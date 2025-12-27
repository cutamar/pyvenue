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

    def cancel(self, order_id: OrderId) -> None:
        order = self.orders.pop(order_id, None)
        if order is None:
            raise RuntimeError(
                f"Order {order_id} not found in expected level {self.price.ticks}"
            )

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
    orders_by_id: dict[OrderId, tuple[Side, int]]

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

    def place_limit(self, order: RestingOrder) -> list[Fill]:
        price = order.price.ticks
        if order.side == Side.BUY:
            price_level = self._ensure_level(Side.BUY, price)
            price_level.add(order)
        elif order.side == Side.SELL:
            price_level = self._ensure_level(Side.SELL, price)
            price_level.add(order)
        else:
            raise ValueError(f"Invalid side: {order.side}")
        self.orders_by_id[order.order_id] = (order.side, price)
        return self._match(order.side, price, order.remaining)

    def cancel(self, order_id: OrderId) -> None:
        loc = self.orders_by_id.get(order_id, None)
        if loc is None:
            raise RuntimeError(f"Order {order_id} not found")
        side, price = loc
        if side == Side.BUY:
            price_level = self._ensure_level(Side.BUY, price)
            price_level.cancel(order_id)
            self._remove_level_if_empty(Side.BUY, price)
        elif side == Side.SELL:
            price_level = self._ensure_level(Side.SELL, price)
            price_level.cancel(order_id)
            self._remove_level_if_empty(Side.SELL, price)
        else:
            raise RuntimeError(f"Invalid side: {side}")
        self.orders_by_id.pop(order_id, None)

    def _ensure_level(self, side: Side, price_ticks: int) -> PriceLevel:
        if side == Side.BUY:
            if price_ticks not in self.bids:
                bisect.insort(self.bid_prices, price_ticks)
                self.bids[price_ticks] = PriceLevel(price_ticks)
            return self.bids[price_ticks]
        elif side == Side.SELL:
            if price_ticks not in self.asks:
                bisect.insort(self.ask_prices, price_ticks)
                self.asks[price_ticks] = PriceLevel(price_ticks)
            return self.asks[price_ticks]
        else:
            raise ValueError(f"Invalid side: {side}")
    
    def _remove_level_if_empty(self, side: Side, price_ticks: int) -> None:
        if side == Side.BUY:
            if price_ticks in self.bids and not self.bids[price_ticks]:
                del self.bids[price_ticks]
                i = bisect.bisect_left(self.bid_prices, price_ticks)
                if i >= len(self.bid_prices) or self.bid_prices[i] != price_ticks:
                    raise RuntimeError(f"Bid prices out of sync for price: {price_ticks}")
                self.bid_prices.pop(i)
        elif side == Side.SELL:
            if price_ticks in self.asks and not self.asks[price_ticks]:
                del self.asks[price_ticks]
                i = bisect.bisect_left(self.ask_prices, price_ticks)
                if i >= len(self.ask_prices) or self.ask_prices[i] != price_ticks:
                    raise RuntimeError(f"Ask prices out of sync for price: {price_ticks}")
                self.ask_prices.pop(i)
        else:
            raise ValueError(f"Invalid side: {side}")

    def _crosses(self, taker_side: Side, taker_price_ticks: int, best_opp_price_ticks: int) -> bool:
        if taker_side == Side.BUY:
            return taker_price_ticks >= best_opp_price_ticks
        elif taker_side == Side.SELL:
            return taker_price_ticks <= best_opp_price_ticks
        else:
            raise ValueError(f"Invalid side: {taker_side}")
    
    def _match(self, taker_side: Side, taker_price_ticks: int, taker_qty_lots: int) -> tuple[list[Fill], int]:
        if taker_side == Side.BUY:
            best_opp_price_ticks = self.best_ask()
            maker_side = Side.SELL
        elif taker_side == Side.SELL:
            best_opp_price_ticks = self.best_bid()
            maker_side = Side.BUY
        else:
            raise ValueError(f"Invalid side: {taker_side}")
        if best_opp_price_ticks is None:
            return [], taker_qty_lots
        if not self._crosses(taker_side, taker_price_ticks, best_opp_price_ticks):
            return [], taker_qty_lots
        maker_level = self._ensure_level(maker_side, best_opp_price_ticks)
        maker_order = maker_level.peek_oldest()
        fills = []
        # TODO: match next best opposite price level if current is fully matched
        while taker_qty_lots > 0 and maker_order.qty > 0:
            maker_order = maker_level.pop_oldest()
            fill_qty_lots = min(taker_qty_lots, maker_order.qty)
            fills.append(
                Fill(
                    maker_order_id=maker_order.order_id,
                    maker_price=Price(best_opp_price_ticks),
                    qty=fill_qty_lots,
                )
            )
            taker_qty_lots -= fill_qty_lots
            maker_order.qty -= fill_qty_lots
            if maker_order.qty == 0:
                maker_level.cancel(maker_order.order_id)
                self._remove_level_if_empty(maker_side, best_opp_price_ticks)
                maker_order = maker_level.peek_oldest()
        return fills, taker_qty_lots