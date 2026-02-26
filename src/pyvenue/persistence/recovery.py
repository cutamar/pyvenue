from pyvenue.domain.types import Instrument
from pyvenue.persistence.event_store import EventStore
from pyvenue.persistence.snapshot_store import SnapshotStore
from pyvenue.venue import Venue


def recover_venue(
    instruments: list[Instrument],
    event_store: EventStore,
    snapshot_store: SnapshotStore,
    return_stats: bool = True,
) -> Venue:
    pass
