from __future__ import annotations

from dataclasses import dataclass, field
from functools import singledispatchmethod

from pyvenue.domain.commands import Cancel, Command, PlaceLimit
from pyvenue.domain.events import Event, OrderAccepted, OrderCanceled, OrderRejected
from pyvenue.domain.types import Instrument, OrderId
from pyvenue.engine.orderbook import OrderBook
from pyvenue.engine.state import EngineState
from pyvenue.infra import Clock, EventLog, SystemClock


@dataclass(slots=True)
class Engine:
    clock: Clock = field(default_factory=SystemClock)
    book: OrderBook = field(default_factory=OrderBook)
    state: EngineState = field(default_factory=EngineState)
    log: EventLog = field(default_factory=EventLog)
    seq: int = field(default=0)

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
        events = self.handle(command)
        for e in events:
            self.log.append(e)
            self.state.apply(e)
        return events

    @singledispatchmethod
    def handle(self, command: Command) -> list[Event]:
        raise TypeError(f"Unsupported command {type(command)!r}")

    @handle.register
    def _(self, command: PlaceLimit) -> list[Event]:
        if command.qty.lots <= 0:
            return [
                self._reject(command.instrument, command.order_id, "qty must be > 0")
            ]
        if command.price.ticks <= 0:
            return [
                self._reject(command.instrument, command.order_id, "price must be > 0")
            ]
        if command.order_id in self.state.orders:
            return [
                self._reject(command.instrument, command.order_id, "duplicate order_id")
            ]

        seq, ts = self._next_meta()
        return [
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

    @handle.register
    def _(self, command: Cancel) -> list[Event]:
        record = self.state.orders.get(command.order_id)
        if record is None:
            return [
                self._reject(command.instrument, command.order_id, "unknown order_id")
            ]
        if record.status != record.status.ACTIVE:
            return [
                self._reject(
                    command.instrument, command.order_id, "order not cancelable"
                )
            ]

        seq, ts = self._next_meta()
        return [
            OrderCanceled(
                seq=seq,
                ts_ns=ts,
                instrument=command.instrument,
                order_id=command.order_id,
            )
        ]
