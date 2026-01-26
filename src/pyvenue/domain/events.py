from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from pyvenue.domain.types import Instrument, OrderId, Price, Qty, Side


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderAccepted:
    type: Literal["OrderAccepted"] = field(default="OrderAccepted", init=False)
    seq: int
    ts_ns: int
    instrument: Instrument
    order_id: OrderId
    side: Side
    price: Price
    qty: Qty


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderRejected:
    type: Literal["OrderRejected"] = field(default="OrderRejected", init=False)
    seq: int
    ts_ns: int
    instrument: Instrument
    order_id: OrderId
    reason: str


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderCanceled:
    type: Literal["OrderCanceled"] = field(default="OrderCanceled", init=False)
    seq: int
    ts_ns: int
    instrument: Instrument
    order_id: OrderId


@dataclass(frozen=True, slots=True, kw_only=True)
class TradeOccurred:
    type: Literal["TradeOccurred"] = field(default="TradeOccurred", init=False)
    seq: int
    ts_ns: int
    instrument: Instrument
    taker_order_id: OrderId
    maker_order_id: OrderId
    price: Price
    qty: Qty


@dataclass(frozen=True, slots=True, kw_only=True)
class TopOfBookChanged:
    type: Literal["TopOfBookChanged"] = field(default="TopOfBookChanged", init=False)
    seq: int
    ts_ns: int
    instrument: Instrument
    best_bid_ticks: int | None
    best_ask_ticks: int | None


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderRested:
    type: Literal["OrderRested"] = field(default="OrderRested", init=False)
    seq: int
    ts_ns: int
    instrument: Instrument
    order_id: OrderId
    side: Side
    price: Price
    qty: Qty


@dataclass(frozen=True, slots=True, kw_only=True)
class OrderExpired:
    type: Literal["OrderExpired"] = field(default="OrderExpired", init=False)
    seq: int
    ts_ns: int
    instrument: Instrument
    order_id: OrderId
    qty: Qty


Event = (
    OrderAccepted
    | OrderRejected
    | OrderCanceled
    | TradeOccurred
    | TopOfBookChanged
    | OrderRested
    | OrderExpired
)
