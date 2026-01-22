from dataclasses import dataclass

from pyvenue.infra.clock import Clock


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
