from __future__ import annotations

from collections import defaultdict

from pyvenue.domain.commands import Command
from pyvenue.domain.events import Event, OrderRejected
from pyvenue.domain.types import AccountId, Asset, Instrument
from pyvenue.engine.engine import Engine
from pyvenue.infra import Clock, SystemClock


class Venue:
    def __init__(self, instruments: list[Instrument]) -> None:
        self.instruments = instruments
        self.engines: dict[Instrument, Engine] = {
            inst: Engine(inst, next_meta=self._next_meta) for inst in instruments
        }
        self.clock: Clock = SystemClock()
        self.seq = 0

    def _next_meta(self) -> tuple[int, int]:
        self.seq += 1
        return self.seq, self.clock.now_ns()

    def credit(
        self, instrument: Instrument, account: AccountId, asset: Asset, amount: int
    ) -> None:
        self.engines[instrument].state.credit(account, asset, amount)

    def submit(self, command: Command) -> list[Event]:
        events: list[Event] = []
        if command.instrument not in self.engines:
            seq, ts = self._next_meta()
            events.append(
                OrderRejected(
                    seq=seq,
                    ts_ns=ts,
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
        if events:
            venue.seq = max(e.seq for e in events if e.instrument in instruments)
        for event in events:
            instrument_to_events[event.instrument].append(event)
        for instrument in instrument_to_events:
            if instrument not in instruments:
                raise RuntimeError(f"instrument {instrument} not found")
        for instrument, events in instrument_to_events.items():
            venue.engines[instrument] = Engine.replay(
                instrument, events, venue._next_meta, rebuild_book
            )
        return venue
