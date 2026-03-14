from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum

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
    accounts: dict[tuple[AccountId, Asset], int]
    accounts_held: dict[tuple[AccountId, Asset], int]
    base_asset: Asset
    quote_asset: Asset
    fee_schedule: FeeSchedule | None = field(default=None)

    def __init__(
        self,
        base_asset: Asset,
        quote_asset: Asset,
        fee_schedule: FeeSchedule | None = None,
    ) -> None:
        from collections import defaultdict

        self.orders = {}
        self.accounts = defaultdict(int)
        self.accounts_held = defaultdict(int)
        self.base_asset = base_asset
        self.quote_asset = quote_asset
        self.fee_schedule = fee_schedule

    def _log_state(self) -> None:
        logger.debug("Engine state", orders=self.orders)

    def available(self, account: AccountId, asset: Asset) -> int:
        return self.accounts[(account, asset)]

    def held(self, account: AccountId, asset: Asset) -> int:
        return self.accounts_held[(account, asset)]

    def process_trade_fees(self, trade: TradeOccurred) -> None:
        if self.fee_schedule is None:
            return
        if trade.maker_order_id in self.orders:
            maker_order = self.orders[trade.maker_order_id]
            fee = math.ceil(
                (self.fee_schedule.maker_bps * trade.qty.lots * trade.price.ticks)
                / 10_000
            )
            available = max(
                0, self.accounts[(maker_order.account_id, self.fee_schedule.fee_asset)]
            )
            actual_fee = min(fee, available)
            logger.debug(
                "Decreasing fee from maker account",
                account=maker_order.account_id,
                asset=self.fee_schedule.fee_asset,
                qty=actual_fee,
            )
            self.accounts[(maker_order.account_id, self.fee_schedule.fee_asset)] -= (
                actual_fee
            )
            logger.debug(
                "Add fee to fee account",
                account=self.fee_schedule.fee_account,
                asset=self.fee_schedule.fee_asset,
                qty=actual_fee,
            )
            self.accounts[
                (self.fee_schedule.fee_account, self.fee_schedule.fee_asset)
            ] += actual_fee
        if trade.taker_order_id in self.orders:
            taker_order = self.orders[trade.taker_order_id]
            fee = math.ceil(
                (self.fee_schedule.taker_bps * trade.qty.lots * trade.price.ticks)
                / 10_000
            )
            available = max(
                0, self.accounts[(taker_order.account_id, self.fee_schedule.fee_asset)]
            )
            actual_fee = min(fee, available)
            logger.debug(
                "Decreasing fee from taker account",
                account=taker_order.account_id,
                asset=self.fee_schedule.fee_asset,
                qty=actual_fee,
            )
            self.accounts[(taker_order.account_id, self.fee_schedule.fee_asset)] -= (
                actual_fee
            )
            logger.debug(
                "Add fee to fee account",
                account=self.fee_schedule.fee_account,
                asset=self.fee_schedule.fee_asset,
                qty=actual_fee,
            )
            self.accounts[
                (self.fee_schedule.fee_account, self.fee_schedule.fee_asset)
            ] += actual_fee

    def apply_all(self, events: list[Event]) -> None:
        for e in events:
            self.apply(e)

    def apply(self, event: Event) -> None:
        if isinstance(event, OrderAccepted):
            self._apply_order_accepted(event)
        elif isinstance(event, TradeOccurred):
            self._apply_trade_occurred(event)
        elif isinstance(event, OrderCanceled):
            self._apply_order_canceled(event)
        elif isinstance(event, OrderExpired):
            self._apply_order_expired(event)
        elif isinstance(event, OrderRested):
            self._apply_order_rested(event)
        elif isinstance(event, FundsReserved):
            self._apply_funds_reserved(event)
        elif isinstance(event, FundsReleased):
            self._apply_funds_released(event)
        elif isinstance(event, FundsCredited):
            self._apply_funds_credited(event)
        elif isinstance(event, OrderRejected):
            self._apply_order_rejected(event)
        elif isinstance(event, TopOfBookChanged):
            self._apply_top_of_book_changed(event)
        else:
            raise TypeError(f"Unsupported event: {type(event)!r}")

    def _apply_order_accepted(self, event: OrderAccepted) -> None:
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

    def _apply_order_canceled(self, event: OrderCanceled) -> None:
        logger.debug("Applying order canceled event", trade_event=event)
        record = self.orders.get(event.order_id)
        if record is not None:
            record.status = OrderStatus.CANCELED
        self._log_state()

    def _apply_trade_occurred(self, event: TradeOccurred) -> None:
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
                pay_asset, get_asset = self.quote_asset, self.base_asset
                pay_qty, get_qty = total_price, event.qty.lots
            else:
                pay_asset, get_asset = self.base_asset, self.quote_asset
                pay_qty, get_qty = event.qty.lots, total_price

            logger.debug(
                "Trade ledger update", account=record.account_id, is_maker=is_maker
            )

            # Everyone gets what they bought into their available balance
            self.accounts[(record.account_id, get_asset)] += get_qty

            # Takers pay from available, Makers pay from reserved/held
            if is_maker:
                self.accounts_held[(record.account_id, pay_asset)] -= pay_qty
            else:
                self.accounts[(record.account_id, pay_asset)] -= pay_qty
        self.process_trade_fees(event)

        self._log_state()

    def _apply_order_expired(self, event: OrderExpired) -> None:
        logger.debug("Applying order expired event", trade_event=event)
        record = self.orders.get(event.order_id)
        if record is not None:
            record.status = OrderStatus.EXPIRED
        self._log_state()

    def _apply_order_rejected(self, event: OrderRejected) -> None:
        # nothing to do in this case
        logger.debug("Applying order rejected event", trade_event=event)

    def _apply_top_of_book_changed(self, event: TopOfBookChanged) -> None:
        logger.debug("Applying top of book changed event", trade_event=event)

    def _apply_order_rested(self, event: OrderRested) -> None:
        logger.debug("Applying order rested event", trade_event=event)

    def _apply_funds_credited(self, event: FundsCredited) -> None:
        logger.debug("Applying funds credited event", trade_event=event)
        self.accounts[(event.account_id, event.asset)] += event.amount.lots

    def _apply_funds_reserved(self, event: FundsReserved) -> None:
        logger.debug("Applying funds reserved event", trade_event=event)
        if self.accounts[(event.account_id, event.asset)] < event.amount.lots:
            raise ValueError(f"Insufficient funds for account {event.account_id!r}")
        self.accounts[(event.account_id, event.asset)] -= event.amount.lots
        self.accounts_held[(event.account_id, event.asset)] += event.amount.lots

    def _apply_funds_released(self, event: FundsReleased) -> None:
        logger.debug("Applying funds released event", trade_event=event)
        if self.accounts_held[(event.account_id, event.asset)] < event.amount.lots:
            raise ValueError(
                f"Insufficient held funds for account {event.account_id!r}"
            )
        self.accounts_held[(event.account_id, event.asset)] -= event.amount.lots
        self.accounts[(event.account_id, event.asset)] += event.amount.lots
