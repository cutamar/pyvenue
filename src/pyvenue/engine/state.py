from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from functools import singledispatchmethod

from structlog import get_logger

from pyvenue.domain.events import (
    Event,
    FundsCredited,
    FundsReleased,
    FundsReserved,
    OrderAccepted,
    OrderCanceled,
    OrderExpired,
    OrderRejected,
    OrderRested,
    TopOfBookChanged,
    TradeOccurred,
)
from pyvenue.domain.types import (
    AccountId,
    Asset,
    FeeSchedule,
    Instrument,
    OrderId,
    Price,
    Qty,
    Side,
)

logger = get_logger()


class OrderStatus(str, Enum):
    ACTIVE = "ACTIVE"
    CANCELED = "CANCELED"
    FILLED = "FILLED"
    EXPIRED = "EXPIRED"


@dataclass(slots=True, kw_only=True)
class OrderRecord:
    instrument: Instrument
    order_id: OrderId
    account_id: AccountId
    side: Side
    price: Price
    qty: Qty
    remaining: Qty
    status: OrderStatus


@dataclass(slots=True)
class EngineState:
    orders: dict[OrderId, OrderRecord]
    accounts: dict[AccountId, dict[Asset, int]]
    accounts_held: dict[AccountId, dict[Asset, int]]
    base_asset: Asset
    quote_asset: Asset
    fee_schedule: FeeSchedule | None = field(default=None)

    def __init__(
        self,
        base_asset: Asset,
        quote_asset: Asset,
        fee_schedule: FeeSchedule | None = None,
    ) -> None:
        self.orders = {}
        self.accounts = {}
        self.accounts_held = {}
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.fee_schedule = fee_schedule

    def _log_state(self) -> None:
        logger.debug("Engine state", orders=self.orders)

    def available(self, account: AccountId, asset: Asset) -> int:
        return self.accounts[account][asset]

    def held(self, account: AccountId, asset: Asset) -> int:
        return self.accounts_held[account][asset]

    def process_trade_fees(self, trade: TradeOccurred) -> None:
        if self.fee_schedule is None:
            return
        if trade.maker_order_id in self.orders:
            maker_order = self.orders[trade.maker_order_id]
            fee = math.ceil(
                (self.fee_schedule.maker_bps * trade.qty.lots * trade.price.ticks)
                / 10_000
            )
            logger.debug(
                "Decreasing fee from maker account",
                account=maker_order.account_id,
                asset=self.fee_schedule.fee_asset,
                qty=fee,
            )
            self.accounts[maker_order.account_id][self.fee_schedule.fee_asset] -= fee
            logger.debug(
                "Add fee to fee account",
                account=self.fee_schedule.fee_account,
                asset=self.fee_schedule.fee_asset,
                qty=fee,
            )
            self.accounts[self.fee_schedule.fee_account][
                self.fee_schedule.fee_asset
            ] += fee
        if trade.taker_order_id in self.orders:
            taker_order = self.orders[trade.taker_order_id]
            fee = math.ceil(
                (self.fee_schedule.taker_bps * trade.qty.lots * trade.price.ticks)
                / 10_000
            )
            logger.debug(
                "Decreasing fee from taker account",
                account=taker_order.account_id,
                asset=self.fee_schedule.fee_asset,
                qty=fee,
            )
            self.accounts[taker_order.account_id][self.fee_schedule.fee_asset] -= fee
            logger.debug(
                "Add fee to fee account",
                account=self.fee_schedule.fee_account,
                asset=self.fee_schedule.fee_asset,
                qty=fee,
            )
            self.accounts[self.fee_schedule.fee_account][
                self.fee_schedule.fee_asset
            ] += fee

    def apply_all(self, events: list[Event]) -> None:
        for e in events:
            self.apply(e)

    @singledispatchmethod
    def apply(self, event: Event) -> None:
        raise TypeError(f"Unsupported event: {type(event)!r}")

    @apply.register
    def _(self, event: OrderAccepted) -> None:
        logger.debug("Applying order accepted event", trade_event=event)
        self.orders[event.order_id] = OrderRecord(
            instrument=event.instrument,
            order_id=event.order_id,
            account_id=event.account_id,
            side=event.side,
            price=event.price,
            qty=event.qty,
            remaining=event.qty,
            status=OrderStatus.ACTIVE,
        )
        self._log_state()

    @apply.register
    def _(self, event: OrderCanceled) -> None:
        logger.debug("Applying order canceled event", trade_event=event)
        record = self.orders.get(event.order_id)
        if record is not None:
            record.status = OrderStatus.CANCELED
        self._log_state()

    @apply.register
    def _(self, event: TradeOccurred) -> None:
        logger.debug("Applying trade occurred event", trade_event=event)
        for order_id in (event.taker_order_id, event.maker_order_id):
            record = self.orders.get(order_id)
            if record is None:
                continue
            is_maker = order_id == event.maker_order_id
            new_remaining = record.remaining.lots - event.qty.lots
            record.remaining = Qty(max(0, new_remaining))
            if record.remaining.lots == 0 and record.status == OrderStatus.ACTIVE:
                record.status = OrderStatus.FILLED

            total_price = event.qty.lots * event.price.ticks
            if record.side == Side.BUY:
                logger.debug(
                    "Decreasing account {account} for asset {asset} by {qty}",
                    account=record.account_id,
                    asset=self.base_asset,
                    qty=total_price,
                    is_maker=is_maker,
                )
                if is_maker:
                    self.accounts_held[record.account_id][self.base_asset] -= (
                        total_price
                    )
                else:
                    self.accounts[record.account_id][self.base_asset] -= total_price
                logger.debug(
                    "Increasing account {account} for asset {asset} by {qty}",
                    account=record.account_id,
                    asset=self.quote_asset,
                    qty=event.qty.lots,
                    is_maker=is_maker,
                )
                self.accounts[record.account_id][self.quote_asset] += event.qty.lots
            else:
                logger.debug(
                    "Increasing account {account} for asset {asset} by {qty}",
                    account=record.account_id,
                    asset=self.base_asset,
                    qty=total_price,
                    is_maker=is_maker,
                )
                self.accounts[record.account_id][self.base_asset] += total_price
                logger.debug(
                    "Decreasing account {account} for asset {asset} by {qty}",
                    account=record.account_id,
                    asset=self.quote_asset,
                    qty=event.qty.lots,
                    is_maker=is_maker,
                )
                if is_maker:
                    self.accounts_held[record.account_id][self.quote_asset] -= (
                        event.qty.lots
                    )
                else:
                    self.accounts[record.account_id][self.quote_asset] -= event.qty.lots
        self.process_trade_fees(event)

        self._log_state()

    @apply.register
    def _(self, event: OrderExpired) -> None:
        logger.debug("Applying order expired event", trade_event=event)
        record = self.orders.get(event.order_id)
        if record is not None:
            record.status = OrderStatus.EXPIRED
        self._log_state()

    @apply.register
    def _(self, event: OrderRejected) -> None:
        # nothing to do in this case
        logger.debug("Applying order rejected event", trade_event=event)

    @apply.register
    def _(self, event: TopOfBookChanged) -> None:
        logger.debug("Applying top of book changed event", trade_event=event)

    @apply.register
    def _(self, event: OrderRested) -> None:
        logger.debug("Applying order rested event", trade_event=event)

    @apply.register
    def _(self, event: FundsCredited) -> None:
        logger.debug("Applying funds credited event", trade_event=event)
        if event.account_id not in self.accounts:
            self.accounts[event.account_id] = {}
        if self.base_asset not in self.accounts[event.account_id]:
            self.accounts[event.account_id][self.base_asset] = 0
        if self.quote_asset not in self.accounts[event.account_id]:
            self.accounts[event.account_id][self.quote_asset] = 0

        self.accounts[event.account_id][event.asset] += event.amount.lots

        if event.account_id not in self.accounts_held:
            self.accounts_held[event.account_id] = {}
        if self.base_asset not in self.accounts_held[event.account_id]:
            self.accounts_held[event.account_id][self.base_asset] = 0
        if self.quote_asset not in self.accounts_held[event.account_id]:
            self.accounts_held[event.account_id][self.quote_asset] = 0

    @apply.register
    def _(self, event: FundsReserved) -> None:
        logger.debug("Applying funds reserved event", trade_event=event)
        if event.account_id not in self.accounts:
            raise ValueError(f"Account {event.account_id!r} not found")
        if event.asset not in self.accounts[event.account_id]:
            raise ValueError(
                f"Asset {event.asset!r} not found for account {event.account_id!r}"
            )
        if self.accounts[event.account_id][event.asset] < event.amount.lots:
            raise ValueError(f"Insufficient funds for account {event.account_id!r}")
        self.accounts[event.account_id][event.asset] -= event.amount.lots
        if event.account_id not in self.accounts_held:
            self.accounts_held[event.account_id] = {}
        if event.asset not in self.accounts_held[event.account_id]:
            self.accounts_held[event.account_id][event.asset] = 0
        self.accounts_held[event.account_id][event.asset] += event.amount.lots

    @apply.register
    def _(self, event: FundsReleased) -> None:
        logger.debug("Applying funds released event", trade_event=event)
        if event.account_id not in self.accounts_held:
            raise ValueError(f"Account {event.account_id!r} not found")
        if event.asset not in self.accounts_held[event.account_id]:
            raise ValueError(
                f"Asset {event.asset!r} not found for account {event.account_id!r}"
            )
        if self.accounts_held[event.account_id][event.asset] < event.amount.lots:
            raise ValueError(
                f"Insufficient held funds for account {event.account_id!r}"
            )
        self.accounts_held[event.account_id][event.asset] -= event.amount.lots
        if event.account_id not in self.accounts:
            raise ValueError(f"Account {event.account_id!r} not found")
        if event.asset not in self.accounts[event.account_id]:
            raise ValueError(
                f"Asset {event.asset!r} not found for account {event.account_id!r}"
            )
        self.accounts[event.account_id][event.asset] += event.amount.lots
