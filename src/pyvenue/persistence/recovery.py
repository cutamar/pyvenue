from dataclasses import dataclass

from pyvenue.domain.types import Instrument
from pyvenue.persistence.event_store import EventStore
from pyvenue.persistence.snapshot_store import SnapshotStore
from pyvenue.venue import Venue


@dataclass(slots=True)
class RecoveryStats:
    loaded_snapshot: bool = False
    snapshot_seq: int = 0
    replayed_events: int = 0
    replayed_from_seq: int = 0


def recover_venue(
    instruments: list[Instrument],
    event_store: EventStore,
    snapshot_store: SnapshotStore,
    return_stats: bool = False,
) -> Venue | tuple[Venue, RecoveryStats]:
    import pickle

    stats = RecoveryStats()
    snap = snapshot_store.load_latest()
    if snap is not None:
        # load from snapshot
        v = Venue(instruments)
        engines = pickle.loads(snap.data)
        for inst, eng in engines.items():
            if inst in instruments:
                v.engines[inst] = eng
        v.seq = snap.seq
        start_seq = snap.seq + 1
        stats.loaded_snapshot = True
        stats.snapshot_seq = snap.seq
    else:
        v = Venue(instruments)
        start_seq = 1

    stats.replayed_from_seq = start_seq
    events = []
    for ev in event_store.iter_from(start_seq):
        events.append(ev)

    # replay events into the loaded venue/engines
    # wait, venue replay requires passing in the events list. But our Venue.replay makes a fresh Venue!
    # Actually, we can just apply events directly!
    # Because Venue.replay(cls) creates new engines. Here we already have engines from snapshot!
    for ev in events:
        if ev.instrument in v.engines:
            v.engines[ev.instrument].log.append(ev)
            v.engines[ev.instrument].state.apply(ev)
            v.engines[ev.instrument].book.apply_event(ev)
            v.seq = max(v.seq, ev.seq)
            stats.replayed_events += 1

    if return_stats:
        return v, stats
    return v
