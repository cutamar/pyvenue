from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import NewType

OrderId = NewType("OrderId", str)
Instrument = NewType("Instrument", str)
AccountId = NewType("AccountId", str)
Asset = NewType("Asset", str)


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"

    def opposite(self) -> Side:
        if self == Side.BUY:
            return Side.SELL
        return Side.BUY


@dataclass(frozen=True, slots=True)
class Price:
    """Price in integer ticks."""

    ticks: int


@dataclass(frozen=True, slots=True)
class Qty:
    """Quantity in integer lots."""

    lots: int


class TimeInForce(str, Enum):
    GTC = "GTC"
    IOC = "IOC"
    FOK = "FOK"
