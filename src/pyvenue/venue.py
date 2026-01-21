from __future__ import annotations

from collections import defaultdict

from pyvenue.domain.commands import Command
from pyvenue.domain.events import Event, OrderRejected
from pyvenue.domain.types import Instrument
from pyvenue.engine.engine import Engine


class Venue:
    def __init__(self, instruments: list[Instrument]) -> None:
        self.instruments = instruments
        self.engines: dict[Instrument, Engine] = {
            inst: Engine(inst) for inst in instruments
        }

    def submit(self, command: Command) -> list[Event]:
        events: list[Event] = []
        if command.instrument not in self.engines:
            # TODO: sequence number
            events.append(
                OrderRejected(
                    seq=1,
                    ts_ns=1,
                    instrument=command.instrument,
                    order_id=command.order_id,
                    reason="instrument not found",
                )
            )
        else:
            events.extend(self.engines[command.instrument].submit(command))
        return events

    @classmethod
    def replay(
        cls,
        instruments: list[Instrument],
        events: list[Event],
        rebuild_book: bool = True,
    ) -> Venue:
        venue = cls(instruments)
        instrument_to_events = defaultdict(list)
        for event in events:
            instrument_to_events[event.instrument].append(event)
        for instrument, events in instrument_to_events.items():
            venue.engines[instrument] = Engine.replay(instrument, events, rebuild_book)
        return venue
