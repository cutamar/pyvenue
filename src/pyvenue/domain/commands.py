from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from pyvenue.domain.types import Instrument, OrderId, Price, Qty, Side


@dataclass(frozen=True, slots=True)
class PlaceLimit:
    type: Literal["PlaceLimit"] = "PlaceLimit"
    instrument: Instrument = field(default_factory=Instrument)
    order_id: OrderId = field(default_factory=OrderId)
    side: Side = field(default=Side.BUY)
    price: Price = field(default_factory=lambda: Price(0))
    qty: Qty = field(default_factory=lambda: Qty(0))
