from dataclasses import dataclass

from pyvenue.domain.events import FundsCredited
from pyvenue.domain.types import AccountId, Asset, Instrument, Price
from pyvenue.engine.engine import Engine
from pyvenue.infra.clock import Clock
from pyvenue.venue import Venue


@dataclass(frozen=True, slots=True)
class FixedClock(Clock):
    t: int = 123

    def now_ns(self) -> int:
        return self.t


class NextMeta:
    def __init__(self, clock: Clock | None = None):
        self.seq = 0
        self.clock = clock or FixedClock(123)

    def __call__(self) -> tuple[int, int]:
        self.seq += 1
        return self.seq, self.clock.now_ns()


def engine_with_balances(
    instrument: Instrument, balances: dict[str, dict[str, int]], next_meta=None
) -> Engine:
    """
    Test-driven API: You implement this by either:
      - letting Engine accept initial_balances=..., OR
      - providing a helper/factory in tests that seeds EngineState/ledger.

    balances example:
      {"alice": {"USD": 10_000, "BTC": 0}, "bob": {"USD": 0, "BTC": 5}}
    """
    if next_meta is None:
        next_meta = NextMeta()
    e = Engine(instrument=instrument, next_meta=next_meta)

    for acct, assets in balances.items():
        for asset, amt in assets.items():
            ev = FundsCredited(
                seq=-1,
                ts_ns=0,
                instrument=Instrument(instrument),
                account_id=AccountId(acct),
                asset=Asset(asset),
                amount=Price(amt),
            )
            e.log.append(ev)
            e.state.apply(ev)

    return e


def venue_with_balances(
    instruments: list[Instrument], balances: dict[str, dict[str, dict[str, int]]]
) -> Venue:
    v = Venue(instruments=instruments)
    for instrument, instrument_balances in balances.items():
        for acct, assets in instrument_balances.items():
            for asset, amt in assets.items():
                ev = FundsCredited(
                    seq=-1,
                    ts_ns=0,
                    instrument=Instrument(instrument),
                    account_id=AccountId(acct),
                    asset=Asset(asset),
                    amount=Price(amt),
                )
                v.engines[Instrument(instrument)].log.append(ev)
                v.engines[Instrument(instrument)].state.apply(ev)
    return v
