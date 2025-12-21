from __future__ import annotations

from dataclasses import dataclass, field
from functools import singledispatchmethod

from pyvenue.domain.commands import Cancel, Command, PlaceLimit
from pyvenue.domain.events import Event, OrderRejected
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
    
    def _reject(self, instrument: Instrument, order_id: OrderId, reason: str) -> OrderRejected:
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
        pass

    @handle.register
    def _(self, command: Cancel) -> list[Event]:
        pass
