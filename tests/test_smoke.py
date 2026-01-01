from __future__ import annotations

from pyvenue.domain.types import Instrument
from pyvenue.engine import Engine


def test_import_and_construct_engine():
    Engine(instrument=Instrument("BTC-USD"))
