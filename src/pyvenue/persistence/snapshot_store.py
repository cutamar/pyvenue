from dataclasses import dataclass
from pathlib import Path


@dataclass
class Snapshot:
    seq: int
    ts_ns: int
    data: bytes


class SnapshotStore:
    def save(self, snapshot: Snapshot) -> None:
        pass

    def load_latest(self) -> Snapshot | None:
        pass


class JsonSnapshotStore(SnapshotStore):
    def __init__(self, path: Path) -> None:
        self.path = path

    def save(self, snapshot: Snapshot) -> None:
        pass

    def load_latest(self) -> Snapshot | None:
        pass
