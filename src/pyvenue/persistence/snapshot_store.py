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
        self.path.mkdir(parents=True, exist_ok=True)

    def save(self, snapshot: Snapshot) -> None:
        import base64
        import json

        with open(self.path / f"snapshot_{snapshot.seq}.json", "w") as f:
            d = {
                "seq": snapshot.seq,
                "ts_ns": snapshot.ts_ns,
                "data": base64.b64encode(snapshot.data).decode("utf-8"),
            }
            json.dump(d, f)

    def load_latest(self) -> Snapshot | None:
        import base64
        import json

        files = list(self.path.glob("snapshot_*.json"))
        if not files:
            return None
        latest_file = max(files, key=lambda p: int(p.stem.split("_")[1]))
        with open(latest_file) as f:
            d = json.load(f)
            return Snapshot(
                seq=d["seq"], ts_ns=d["ts_ns"], data=base64.b64decode(d["data"])
            )
