from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import NewType

OrderId = NewType("OrderId", str)
Instrument = NewType("Instrument", str)


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True, slots=True)
class Price:
    """Price in integer ticks."""
    
    ticks: int


@dataclass(frozen=True, slots=True)
class Qty:
    """Quantity in integer lots."""

    lots: int
