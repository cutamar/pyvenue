from collections.abc import Iterable
from pathlib import Path

from pyvenue.domain.events import Event


class EventStore:
    def iter_from(self, seq: int) -> Iterable[Event]:
        pass

    def append(self, events: list[Event]) -> None:
        pass

    def last_seq(self) -> int:
        pass


class JsonlEventStore(EventStore):
    def __init__(self, path: Path) -> None:
        self.path = path
        import json

        self._json = json

    def _event_to_dict(self, event: Event) -> dict:
        import dataclasses
        from enum import Enum

        d = {"__type__": event.__class__.__name__}
        for f in dataclasses.fields(event):
            val = getattr(event, f.name)
            if hasattr(val, "lots"):
                d[f.name] = val.lots
            elif hasattr(val, "ticks"):
                d[f.name] = val.ticks
            elif isinstance(val, Enum):
                d[f.name] = val.value
            else:
                d[f.name] = val
        return d

    def _dict_to_event(self, d: dict) -> Event:
        from pyvenue.domain import events as ev_module
        from pyvenue.domain.types import (
            AccountId,
            Asset,
            Instrument,
            OrderId,
            Price,
            Qty,
            Side,
        )

        cls_name = d.pop("__type__")
        cls = getattr(ev_module, cls_name)
        # Reconstruct fields based on annotations
        import inspect

        sig = inspect.signature(cls)
        kwargs = {}
        for k, v in d.items():
            if k not in sig.parameters:
                continue
            anno = sig.parameters[k].annotation
            if anno == "Qty" or anno == Qty:
                kwargs[k] = Qty(v)
            elif anno == "Price" or anno == Price:
                kwargs[k] = Price(v)
            elif anno == "Side" or anno == Side:
                kwargs[k] = Side(v)
            elif anno == "Instrument" or anno == Instrument:
                kwargs[k] = Instrument(v)
            elif anno == "AccountId" or anno == AccountId:
                kwargs[k] = AccountId(v)
            elif anno == "OrderId" or anno == OrderId:
                kwargs[k] = OrderId(v)
            elif anno == "Asset" or anno == Asset:
                kwargs[k] = Asset(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)

    def iter_from(self, seq: int) -> Iterable[Event]:
        if not self.path.exists():
            return
        with open(self.path) as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    d = self._json.loads(line)
                except Exception:
                    continue  # Ignore trailing partial lines
                if "__type__" not in d:
                    continue
                ev = self._dict_to_event(d)
                if ev.seq >= seq:
                    yield ev

    def append(self, events: list[Event]) -> None:
        with open(self.path, "a") as f:
            for ev in events:
                f.write(self._json.dumps(self._event_to_dict(ev)) + "\n")

    def last_seq(self) -> int:
        seq = 0
        for ev in self.iter_from(0):
            seq = max(seq, ev.seq)
        return seq
