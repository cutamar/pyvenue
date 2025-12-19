from __future__ import annotations

from dataclasses import dataclass

from pyvenue.domain.events import Event


@dataclass(slots=True)
class EventLog:
    """In-memory log which gets appended."""

    _events: list[Event]

    def __init__(self) -> None:
        self._events = []

    def append(self, event: Event) -> None:
        self._events.append(event)

    def extend(self, events: list[Event]) -> None:
        self._events.extend(events)

    def all(self) -> list[Event]:
        return list(self._events)
