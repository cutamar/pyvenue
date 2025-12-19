from __future__ import annotations

from dataclasses import dataclass, field

from pyvenue.engine.orderbook import OrderBook
from pyvenue.engine.state import EngineState
from pyvenue.infra import Clock, SystemClock


@dataclass(slots=True)
class Engine:
    clock: Clock = field(default_factory=SystemClock)
    book: OrderBook = field(default_factory=OrderBook)
    state: EngineState = field(default_factory=EngineState)

    def submit(self, command: object) -> list[object]:
        raise NotImplementedError
