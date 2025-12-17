from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from pyvenue.engine import OrderBook
from pyvenue.infra import Clock, SystemClock


@dataclass(slots=True)
class Engine:
    clock: Clock = field(default_factory=SystemClock)

    def __post_init__(self) -> None:
        self._book = OrderBook()

    def submit(self, command: object) -> List[object]:
        raise NotImplementedError
