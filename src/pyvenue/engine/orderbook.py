from __future__ import annotations

import bisect
from collections import OrderedDict
from dataclasses import dataclass, field

import structlog

from pyvenue.domain.events import (
    Event,
    OrderRested,
)
from pyvenue.domain.types import Instrument, OrderId, Price, Qty, Side

logger = structlog.get_logger(__name__)


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
    orders: OrderedDict[OrderId, RestingOrder] = field(default_factory=OrderedDict)
    logger: structlog.BoundLogger = field(init=False)

    def __post_init__(self) -> None:
        self.logger = logger.bind(
            _component=self.__class__.__name__,
            price=self.price.ticks,
        )

    def __len__(self) -> int:
        return len(self.orders)

    def __bool__(self) -> bool:
        return bool(self.orders)

    def add(self, order: RestingOrder) -> None:
        self.logger.debug("Adding order to level", order=order)
        self.orders[order.order_id] = order

    def cancel(self, order_id: OrderId) -> bool:
        self.logger.debug("Canceling order from level", order_id=order_id)
        order = self.orders.pop(order_id, None)
        return order is not None

    def peek_oldest(self) -> RestingOrder | None:
        return next(iter(self.orders.values()), None)

    def pop_oldest(self) -> RestingOrder:
        self.logger.debug(
            "Popping oldest order from level",
            order=self.peek_oldest(),
        )
        _, order = self.orders.popitem(last=False)
        return order


@dataclass(slots=True)
class OrderBook:
    """An order book for a single instrument."""

    instrument: Instrument
    bids: dict[int, PriceLevel] = field(default_factory=dict)
    asks: dict[int, PriceLevel] = field(default_factory=dict)
    bid_prices: list[int] = field(default_factory=list)
    ask_prices: list[int] = field(default_factory=list)
    orders_by_id: dict[OrderId, tuple[Side, int]] = field(default_factory=dict)
    logger: structlog.BoundLogger = field(init=False)

    def __post_init__(self) -> None:
        self.logger = logger.bind(
            _component=self.__class__.__name__,
        )

    def best_bid(self) -> int | None:
        return self.bid_prices[-1] if self.bid_prices else None

    def best_ask(self) -> int | None:
        return self.ask_prices[0] if self.ask_prices else None

    def top_of_book(self) -> tuple[int | None, int | None]:
        return self.best_bid(), self.best_ask()

    def _log_book(self) -> None:
        self.logger.debug(
            "Order book state",
            num_bids=len(self.bids),
            num_asks=len(self.asks),
            num_orders=len(self.orders_by_id),
            num_bid_levels=len(self.bid_prices),
            num_ask_levels=len(self.ask_prices),
            best_bid=self.best_bid(),
            best_ask=self.best_ask(),
        )

    def apply_event(self, event: Event) -> None:
        if isinstance(event, OrderRested):
            self._rest(
                RestingOrder(
                    order_id=event.order_id,
                    instrument=event.instrument,
                    side=event.side,
                    price=event.price,
                    remaining=event.qty,
                )
            )
        # handle TradeOccurred and OrderCanceled
        else:
            raise ValueError(f"Event type {event.__class__.__name__} not implemented")

    def _rest(self, order: RestingOrder) -> None:
        self.logger.debug("Resting order in the book", order=order)
        price = order.price.ticks
        self.orders_by_id[order.order_id] = (order.side, price)
        if order.side == Side.BUY:
            price_level = self._ensure_level(Side.BUY, price)
        elif order.side == Side.SELL:
            price_level = self._ensure_level(Side.SELL, price)
        else:
            raise ValueError(f"Invalid side: {order.side}")
        price_level.add(order)

    def place_limit(self, order: RestingOrder) -> tuple[list[Fill], int]:
        self._log_book()
        self.logger.debug("Placing limit order in the book", order=order)
        if order.instrument != self.instrument:
            raise ValueError("Order instrument does not match book instrument")
        price = order.price.ticks
        fills, remaining = self._match(order.side, price, order.remaining.lots)
        if remaining > 0:
            # place remaining order
            order.remaining = Qty(remaining)
            self._rest(order)
        self.logger.debug("Limit order placed in the book", remaining=remaining)
        self._log_book()
        return fills, remaining

    def cancel(self, order_id: OrderId) -> bool:
        self._log_book()
        self.logger.debug("Canceling order in the book", order_id=order_id)
        loc = self.orders_by_id.get(order_id, None)
        if loc is None:
            return False
        side, price = loc
        if side == Side.BUY:
            price_level = self.bids.get(price, None)
            if price_level is None:
                return False
            if not price_level.cancel(order_id):
                return False
            self._remove_level_if_empty(Side.BUY, price)
        elif side == Side.SELL:
            price_level = self.asks.get(price, None)
            if price_level is None:
                return False
            if not price_level.cancel(order_id):
                return False
            self._remove_level_if_empty(Side.SELL, price)
        else:
            raise RuntimeError(f"Invalid side: {side}")
        self.orders_by_id.pop(order_id, None)
        self._log_book()
        return True

    def _ensure_level(self, side: Side, price_ticks: int) -> PriceLevel:
        if side == Side.BUY:
            if price_ticks not in self.bids:
                bisect.insort(self.bid_prices, price_ticks)
                self.bids[price_ticks] = PriceLevel(Price(price_ticks))
            return self.bids[price_ticks]
        elif side == Side.SELL:
            if price_ticks not in self.asks:
                bisect.insort(self.ask_prices, price_ticks)
                self.asks[price_ticks] = PriceLevel(Price(price_ticks))
            return self.asks[price_ticks]
        else:
            raise ValueError(f"Invalid side: {side}")

    def _get_level(self, side: Side, price_ticks: int) -> PriceLevel | None:
        if side == Side.BUY:
            return self.bids.get(price_ticks, None)
        elif side == Side.SELL:
            return self.asks.get(price_ticks, None)
        else:
            raise ValueError(f"Invalid side: {side}")

    def _remove_level_if_empty(self, side: Side, price_ticks: int) -> None:
        if side == Side.BUY:
            if price_ticks in self.bids and not self.bids[price_ticks]:
                del self.bids[price_ticks]
                i = bisect.bisect_left(self.bid_prices, price_ticks)
                if i >= len(self.bid_prices) or self.bid_prices[i] != price_ticks:
                    raise RuntimeError(
                        f"Bid prices out of sync for price: {price_ticks}"
                    )
                self.bid_prices.pop(i)
        elif side == Side.SELL:
            if price_ticks in self.asks and not self.asks[price_ticks]:
                del self.asks[price_ticks]
                i = bisect.bisect_left(self.ask_prices, price_ticks)
                if i >= len(self.ask_prices) or self.ask_prices[i] != price_ticks:
                    raise RuntimeError(
                        f"Ask prices out of sync for price: {price_ticks}"
                    )
                self.ask_prices.pop(i)
        else:
            raise ValueError(f"Invalid side: {side}")

    def _crosses(
        self, taker_side: Side, taker_price_ticks: int, best_opp_price_ticks: int
    ) -> bool:
        if taker_side == Side.BUY:
            return taker_price_ticks >= best_opp_price_ticks
        elif taker_side == Side.SELL:
            return taker_price_ticks <= best_opp_price_ticks
        else:
            raise ValueError(f"Invalid side: {taker_side}")

    def _match(
        self, taker_side: Side, taker_price_ticks: int, taker_qty_lots: int
    ) -> tuple[list[Fill], int]:
        if taker_side == Side.BUY:
            maker_side = Side.SELL
            get_best_opp = self.best_ask
        elif taker_side == Side.SELL:
            maker_side = Side.BUY
            get_best_opp = self.best_bid
        else:
            raise ValueError(f"Invalid side: {taker_side}")

        fills = []

        while taker_qty_lots > 0:
            best_opp_price_ticks = get_best_opp()
            if best_opp_price_ticks is None:
                break

            if not self._crosses(taker_side, taker_price_ticks, best_opp_price_ticks):
                break

            maker_level = self._get_level(maker_side, best_opp_price_ticks)
            if maker_level is None:
                raise RuntimeError(f"No level found for price: {best_opp_price_ticks}")

            # Match against this level until empty or taker filled

            maker_order = maker_level.peek_oldest()
            while taker_qty_lots > 0 and maker_order is not None:
                if maker_order.remaining.lots <= 0:
                    # Should not happen if logic is correct, but for safety
                    maker_level.pop_oldest()
                    maker_order = maker_level.peek_oldest()
                    continue

                fill_qty_lots = min(taker_qty_lots, maker_order.remaining.lots)
                fills.append(
                    Fill(
                        maker_order_id=maker_order.order_id,
                        maker_price=Price(best_opp_price_ticks),
                        qty=Qty(fill_qty_lots),
                    )
                )
                taker_qty_lots -= fill_qty_lots
                maker_order.remaining = Qty(maker_order.remaining.lots - fill_qty_lots)

                if maker_order.remaining.lots == 0:
                    maker_order = maker_level.pop_oldest()
                    self.orders_by_id.pop(maker_order.order_id, None)
                    # We modified the book, so we need to peek again
                    maker_order = maker_level.peek_oldest()
                else:
                    # Taker fully filled, maker has remaining
                    pass

            # If level is empty, remove it
            self._remove_level_if_empty(maker_side, best_opp_price_ticks)

        return fills, taker_qty_lots
