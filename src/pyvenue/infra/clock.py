from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Clock:
    """Clock abstraction so we can run in real-time or simulated time later."""

    def now_ns(self) -> int:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class SystemClock(Clock):
    def now_ns(self) -> int:
        return time.time_ns()