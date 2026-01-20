from __future__ import annotations

from pyvenue.domain.commands import Command
from pyvenue.domain.events import Event
from pyvenue.domain.types import Instrument
from pyvenue.engine.orderbook import OrderBook


class Venue:
    def __init__(self, instruments: list[Instrument]) -> None:
        self.instruments = instruments
        self.books = {inst: OrderBook(inst) for inst in instruments}

    def submit(self, command: Command) -> list[Event]:
        events: list[Event] = []
        return events

    @classmethod
    def replay(
        cls, instruments: list[Instrument], events: list[Event], rebuild_books: bool
    ) -> Venue:
        return cls(instruments)
