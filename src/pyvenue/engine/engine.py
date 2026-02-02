from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import singledispatchmethod

import structlog

from pyvenue.domain.commands import Cancel, Command, PlaceLimit, PlaceMarket
from pyvenue.domain.events import (
    Event,
    OrderAccepted,
    OrderCanceled,
    OrderExpired,
    OrderRejected,
    OrderRested,
    TopOfBookChanged,
    TradeOccurred,
)
from pyvenue.domain.types import Instrument, OrderId, Price, Qty, Side, TimeInForce
from pyvenue.engine.orderbook import OrderBook, RestingOrder
from pyvenue.engine.state import EngineState, OrderStatus
from pyvenue.infra import EventLog

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class Engine:
    instrument: Instrument
    next_meta: Callable[[], tuple[int, int]]
    state: EngineState = field(default_factory=EngineState)
    log: EventLog = field(default_factory=EventLog)
    book: OrderBook = field(init=False)
    logger: structlog.BoundLogger = field(init=False)

    def __post_init__(self) -> None:
        self.book = OrderBook(self.instrument)
        self.logger = logger.bind(
            _component=self.__class__.__name__,
            instrument=self.instrument,
        )
        self.logger.info(
            "Engine initialized",
        )

    def _reject(
        self, instrument: Instrument, order_id: OrderId, reason: str
    ) -> OrderRejected:
        seq, ts = self.next_meta()
        return OrderRejected(
            seq=seq,
            ts_ns=ts,
            instrument=instrument,
            order_id=order_id,
            reason=reason,
        )

    def submit(self, command: Command) -> list[Event]:
        self.logger.debug("Submitting command", command=command)
        if command.instrument != self.instrument:
            self.logger.warning(
                "Command rejected: instrument mismatch", command=command
            )
            events: list[Event] = [
                self._reject(
                    command.instrument, command.order_id, "instrument mismatch"
                )
            ]
        else:
            top_of_book = self.book.top_of_book()
            events = self.handle(command)
            if top_of_book != self.book.top_of_book():
                seq, ts = self.next_meta()
                events.append(
                    TopOfBookChanged(
                        seq=seq,
                        ts_ns=ts,
                        instrument=self.instrument,
                        best_bid_ticks=self.book.best_bid(),
                        best_ask_ticks=self.book.best_ask(),
                    )
                )
        for e in events:
            self.log.append(e)
            self.state.apply(e)
        return events

    @classmethod
    def replay(
        cls,
        instrument: Instrument,
        events: list[Event],
        next_meta: Callable[[], tuple[int, int]],
        rebuild_book: bool = False,
    ) -> Engine:
        engine = cls(instrument=instrument, next_meta=next_meta)
        for e in events:
            if e.instrument == instrument:
                engine.log.append(e)
                engine.state.apply(e)
                if rebuild_book:
                    engine.book.apply_event(e)
        return engine

    @singledispatchmethod
    def handle(self, command: Command) -> list[Event]:
        self.logger.warning("Unsupported command", command=command)
        raise TypeError(f"Unsupported command {type(command)!r}")

    @handle.register
    def _(self, command: PlaceMarket) -> list[Event]:
        self.logger.debug("Handling PlaceMarket command", command=command)
        if command.qty.lots <= 0:
            self.logger.warning(
                "PlaceMarket command rejected: qty must be > 0", command=command
            )
            events = [
                self._reject(command.instrument, command.order_id, "qty must be > 0")
            ]
        elif command.order_id in self.state.orders:
            self.logger.warning(
                "PlaceMarket command rejected: duplicate order_id", command=command
            )
            events = [
                self._reject(command.instrument, command.order_id, "duplicate order_id")
            ]
        else:
            seq, ts = self.next_meta()
            self.logger.debug("PlaceMarket seq and ts", seq=seq, ts=ts)

            aggressive_price = sys.maxsize if command.side == Side.BUY else 1
            price = Price(aggressive_price)

            events: list[Event] = [
                OrderAccepted(
                    seq=seq,
                    ts_ns=ts,
                    instrument=command.instrument,
                    order_id=command.order_id,
                    side=command.side,
                    price=price,
                    qty=command.qty,
                )
            ]
            fill_events, remaining = self.book.place_limit(
                RestingOrder(
                    order_id=command.order_id,
                    instrument=command.instrument,
                    side=command.side,
                    price=price,
                    remaining=command.qty,
                ),
                rest=False,
            )
            for fill_event in fill_events:
                seq, ts = self.next_meta()
                events.append(
                    TradeOccurred(
                        seq=seq,
                        ts_ns=ts,
                        instrument=command.instrument,
                        taker_order_id=command.order_id,
                        maker_order_id=fill_event.maker_order_id,
                        qty=fill_event.qty,
                        price=fill_event.maker_price,
                    )
                )
            if remaining > 0:
                seq, ts = self.next_meta()
                events.append(
                    OrderExpired(
                        seq=seq,
                        ts_ns=ts,
                        instrument=command.instrument,
                        order_id=command.order_id,
                        qty=Qty(remaining),
                        reason="unfilled",
                    )
                )
        return events

    @handle.register
    def _(self, command: PlaceLimit) -> list[Event]:
        self.logger.debug("Handling PlaceLimit command", command=command)
        if command.qty.lots <= 0:
            self.logger.warning(
                "PlaceLimit command rejected: qty must be > 0", command=command
            )
            events = [
                self._reject(command.instrument, command.order_id, "qty must be > 0")
            ]
        elif command.price.ticks <= 0:
            self.logger.warning(
                "PlaceLimit command rejected: price must be > 0", command=command
            )
            events = [
                self._reject(command.instrument, command.order_id, "price must be > 0")
            ]
        elif command.order_id in self.state.orders:
            self.logger.warning(
                "PlaceLimit command rejected: duplicate order_id", command=command
            )
            events = [
                self._reject(command.instrument, command.order_id, "duplicate order_id")
            ]
        else:
            if command.post_only:
                crosses = False
                if command.side == Side.BUY:
                    best_ask = self.book.best_ask()
                    if best_ask is not None and command.price.ticks >= best_ask:
                        crosses = True
                elif command.side == Side.SELL:
                    best_bid = self.book.best_bid()
                    if best_bid is not None and command.price.ticks <= best_bid:
                        crosses = True

                if crosses:
                    self.logger.warning(
                        "PlaceLimit rejected: post-only would cross", command=command
                    )
                    return [
                        self._reject(
                            command.instrument,
                            command.order_id,
                            "post-only order would cross",
                        )
                    ]

            seq, ts = self.next_meta()
            self.logger.debug("PlaceLimit seq and ts", seq=seq, ts=ts)
            events: list[Event] = [
                OrderAccepted(
                    seq=seq,
                    ts_ns=ts,
                    instrument=command.instrument,
                    order_id=command.order_id,
                    side=command.side,
                    price=command.price,
                    qty=command.qty,
                )
            ]
            rest = command.tif == TimeInForce.GTC
            fill_events, remaining = self.book.place_limit(
                RestingOrder(
                    order_id=command.order_id,
                    instrument=command.instrument,
                    side=command.side,
                    price=command.price,
                    remaining=command.qty,
                ),
                rest=rest,
            )
            for fill_event in fill_events:
                seq, ts = self.next_meta()
                events.append(
                    TradeOccurred(
                        seq=seq,
                        ts_ns=ts,
                        instrument=command.instrument,
                        taker_order_id=command.order_id,
                        maker_order_id=fill_event.maker_order_id,
                        qty=fill_event.qty,
                        price=fill_event.maker_price,
                    )
                )
            if remaining > 0:
                seq, ts = self.next_meta()
                if command.tif == TimeInForce.GTC:
                    events.append(
                        OrderRested(
                            seq=seq,
                            ts_ns=ts,
                            instrument=command.instrument,
                            order_id=command.order_id,
                            side=command.side,
                            price=command.price,
                            qty=Qty(remaining),
                        )
                    )
                elif command.tif == TimeInForce.IOC:
                    events.append(
                        OrderExpired(
                            seq=seq,
                            ts_ns=ts,
                            instrument=command.instrument,
                            order_id=command.order_id,
                            qty=Qty(remaining),
                            reason="IOC",
                        )
                    )
        return events

    @handle.register
    def _(self, command: Cancel) -> list[Event]:
        self.logger.debug("Handling Cancel command", command=command)
        record = self.state.orders.get(command.order_id)
        if record is None:
            self.logger.warning(
                "Cancel command rejected: unknown order_id", command=command
            )
            return [
                self._reject(command.instrument, command.order_id, "unknown order_id")
            ]
        if record.status != OrderStatus.ACTIVE:
            self.logger.warning(
                "Cancel command rejected: order not cancelable", command=command
            )
            return [
                self._reject(
                    command.instrument, command.order_id, "order not cancelable"
                )
            ]

        if not self.book.cancel(command.order_id):
            self.logger.warning(
                "Cancel command rejected: unknown order_id", command=command
            )
            return [
                self._reject(
                    command.instrument, command.order_id, "order_id not in book"
                )
            ]

        seq, ts = self.next_meta()
        self.logger.debug("Cancel seq and ts", seq=seq, ts=ts)
        return [
            OrderCanceled(
                seq=seq,
                ts_ns=ts,
                instrument=command.instrument,
                order_id=command.order_id,
            )
        ]
