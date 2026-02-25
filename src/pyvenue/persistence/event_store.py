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

    def iter_from(self, seq: int) -> Iterable[Event]:
        pass

    def append(self, events: list[Event]) -> None:
        pass

    def last_seq(self) -> int:
        pass
