from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from pyvenue.domain.types import Instrument, OrderId


@dataclass(frozen=True, slots=True)
class OrderAccepted:
    type: Literal["OrderAccepted"] = "OrderAccepted"
    instrument: Instrument = field(default_factory=Instrument)
    order_id: OrderId = field(default_factory=OrderId)


@dataclass(frozen=True, slots=True)
class OrderRejected:
    type: Literal["OrderRejected"] = "OrderRejected"
    instrument: Instrument = field(default_factory=Instrument)
    order_id: OrderId = field(default_factory=OrderId)
