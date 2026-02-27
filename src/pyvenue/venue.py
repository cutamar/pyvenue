from __future__ import annotations

from collections import defaultdict

from pyvenue.domain.commands import Command
from pyvenue.domain.events import Event, OrderRejected
from pyvenue.domain.types import Asset, Instrument
from pyvenue.engine.engine import Engine
from pyvenue.infra import Clock, SystemClock


class _VenueAssets:
    def quote(self, inst: Instrument) -> Asset:
        from pyvenue.domain.types import Asset

        return Asset(inst.split("-")[1])

    def base(self, inst: Instrument) -> Asset:
        from pyvenue.domain.types import Asset

        return Asset(inst.split("-")[0])


class _VenueState:
    def __init__(self, venue: Venue):
        self.venue = venue

    def credit(self, account_id: str, asset: str, amount: int) -> None:
        from pyvenue.domain.events import FundsCredited
        from pyvenue.domain.types import AccountId, Asset, Qty

        account_id = AccountId(str(account_id))
        asset = Asset(str(asset))
        for engine in self.venue.engines.values():
            if asset in (engine.state.base_asset, engine.state.quote_asset):
                ev = FundsCredited(
                    seq=-1,
                    ts_ns=0,
                    instrument=engine.instrument,
                    account_id=account_id,
                    asset=asset,
                    amount=Qty(amount),
                )
                engine.state.apply(ev)

    def digest(self) -> tuple:
        st = []
        for inst, eng in sorted(self.venue.engines.items()):
            st.append(
                (
                    inst,
                    sorted(
                        (k, sorted(v.items())) for k, v in eng.state.accounts.items()
                    ),
                    sorted(
                        (k, sorted(v.items()))
                        for k, v in eng.state.accounts_held.items()
                    ),
                    sorted((k, v.status) for k, v in eng.state.orders.items()),
                )
            )
        return tuple(st)


class Venue:
    def __init__(self, instruments: list[Instrument]) -> None:
        self.instruments = instruments
        self.engines: dict[Instrument, Engine] = {
            inst: Engine(inst, next_meta=self._next_meta) for inst in instruments
        }
        self.clock: Clock = SystemClock()
        self.seq = 0

    @property
    def state(self) -> _VenueState:
        return _VenueState(self)

    @property
    def assets(self) -> _VenueAssets:
        return _VenueAssets()

    def books_digest(self) -> tuple:
        res = []
        for inst, eng in sorted(self.engines.items()):
            res.append(
                (
                    inst,
                    eng.book.best_bid(),
                    eng.book.best_ask(),
                    len(eng.book.orders_by_id),
                )
            )
        return tuple(res)

    def snapshot(self):
        import pickle

        from pyvenue.persistence.snapshot_store import Snapshot

        ts = self.clock.now_ns()
        return Snapshot(seq=self.seq, ts_ns=ts, data=pickle.dumps(self.engines))

    def _next_meta(self) -> tuple[int, int]:
        self.seq += 1
        return self.seq, self.clock.now_ns()

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
