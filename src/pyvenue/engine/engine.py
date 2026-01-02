from __future__ import annotations

from dataclasses import dataclass, field
from functools import singledispatchmethod

import structlog

from pyvenue.domain.commands import Cancel, Command, PlaceLimit
from pyvenue.domain.events import (
    Event,
    OrderAccepted,
    OrderCanceled,
    OrderRejected,
    TradeOccurred,
)
from pyvenue.domain.types import Instrument, OrderId
from pyvenue.engine.orderbook import OrderBook, RestingOrder
from pyvenue.engine.state import EngineState, OrderStatus
from pyvenue.infra import Clock, EventLog, SystemClock

logger = structlog.get_logger(__name__)


@dataclass(slots=True)
class Engine:
    instrument: Instrument
    clock: Clock = field(default_factory=SystemClock)
    state: EngineState = field(default_factory=EngineState)
    log: EventLog = field(default_factory=EventLog)
    seq: int = field(default=0)
    book: OrderBook = field(default_factory=OrderBook)
    logger: structlog.BoundLogger = field(init=False)

    def __post_init__(self) -> None:
        self.logger = logger.bind(
            _component=self.__class__.__name__,
            instrument=self.instrument,
            clock=self.clock,
        )
        self.logger.info(
            "Engine initialized",
            start_seq=self.seq,
        )

    def _next_meta(self) -> tuple[int, int]:
        self.seq += 1
        return self.seq, self.clock.now_ns()

    def _reject(
        self, instrument: Instrument, order_id: OrderId, reason: str
    ) -> OrderRejected:
        seq, ts = self._next_meta()
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
            return [
                self._reject(
                    command.instrument, command.order_id, "instrument mismatch"
                )
            ]
        events = self.handle(command)
        for e in events:
            self.log.append(e)
            self.state.apply(e)
        return events

    @singledispatchmethod
    def handle(self, command: Command) -> list[Event]:
        self.logger.warning("Unsupported command", command=command)
        raise TypeError(f"Unsupported command {type(command)!r}")

    @handle.register
    def _(self, command: PlaceLimit) -> list[Event]:
        self.logger.debug("Handling PlaceLimit command", command=command)
        if command.qty.lots <= 0:
            self.logger.warning(
                "PlaceLimit command rejected: qty must be > 0", command=command
            )
            return [
                self._reject(command.instrument, command.order_id, "qty must be > 0")
            ]
        if command.price.ticks <= 0:
            self.logger.warning(
                "PlaceLimit command rejected: price must be > 0", command=command
            )
            return [
                self._reject(command.instrument, command.order_id, "price must be > 0")
            ]
        if command.order_id in self.state.orders:
            self.logger.warning(
                "PlaceLimit command rejected: duplicate order_id", command=command
            )
            return [
                self._reject(command.instrument, command.order_id, "duplicate order_id")
            ]

        seq, ts = self._next_meta()
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
        fill_events = self.book.place_limit(
            RestingOrder(
                order_id=command.order_id,
                instrument=command.instrument,
                side=command.side,
                price=command.price,
                remaining=command.qty,
            )
        )
        for fill_event in fill_events:
            seq, ts = self._next_meta()
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

        seq, ts = self._next_meta()
        self.logger.debug("Cancel seq and ts", seq=seq, ts=ts)
        return [
            OrderCanceled(
                seq=seq,
                ts_ns=ts,
                instrument=command.instrument,
                order_id=command.order_id,
            )
        ]
