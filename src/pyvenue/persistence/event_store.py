from collections.abc import Iterable
from pathlib import Path

from pyvenue.domain.events import Event


class EventStore:
    def iter_from(self, seq: int) -> Iterable[Event]:
        raise NotImplementedError

    def append(self, events: list[Event]) -> None:
        raise NotImplementedError

    def last_seq(self) -> int:
        raise NotImplementedError


class JsonlEventStore(EventStore):
    def __init__(self, path: Path) -> None:
        self.path = path
        import json

        self._json = json

    def _event_to_dict(self, event: Event) -> dict:
        from pyvenue.domain.events import (
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

        t = getattr(event, "type", event.__class__.__name__)

        if isinstance(event, OrderAccepted):
            return {
                "type": t,
                "seq": event.seq,
                "ts_ns": event.ts_ns,
                "instrument": event.instrument,
                "account_id": event.account_id,
                "order_id": event.order_id,
                "side": event.side.value,
                "price": event.price.ticks,
                "qty": event.qty.lots,
            }
        elif isinstance(event, OrderRejected):
            return {
                "type": t,
                "seq": event.seq,
                "ts_ns": event.ts_ns,
                "instrument": event.instrument,
                "order_id": event.order_id,
                "reason": event.reason,
            }
        elif isinstance(event, OrderCanceled):
            return {
                "type": t,
                "seq": event.seq,
                "ts_ns": event.ts_ns,
                "instrument": event.instrument,
                "order_id": event.order_id,
            }
        elif isinstance(event, TradeOccurred):
            return {
                "type": t,
                "seq": event.seq,
                "ts_ns": event.ts_ns,
                "instrument": event.instrument,
                "taker_order_id": event.taker_order_id,
                "maker_order_id": event.maker_order_id,
                "price": event.price.ticks,
                "qty": event.qty.lots,
            }
        elif isinstance(event, TopOfBookChanged):
            return {
                "type": t,
                "seq": event.seq,
                "ts_ns": event.ts_ns,
                "instrument": event.instrument,
                "best_bid_ticks": event.best_bid_ticks,
                "best_ask_ticks": event.best_ask_ticks,
            }
        elif isinstance(event, OrderRested):
            return {
                "type": t,
                "seq": event.seq,
                "ts_ns": event.ts_ns,
                "instrument": event.instrument,
                "account_id": event.account_id,
                "order_id": event.order_id,
                "side": event.side.value,
                "price": event.price.ticks,
                "qty": event.qty.lots,
            }
        elif isinstance(event, OrderExpired):
            return {
                "type": t,
                "seq": event.seq,
                "ts_ns": event.ts_ns,
                "instrument": event.instrument,
                "order_id": event.order_id,
                "qty": event.qty.lots,
                "reason": event.reason,
            }
        elif isinstance(event, (FundsCredited, FundsReserved, FundsReleased)):
            return {
                "type": t,
                "seq": event.seq,
                "ts_ns": event.ts_ns,
                "instrument": event.instrument,
                "account_id": event.account_id,
                "asset": event.asset,
                "amount": event.amount.lots,
            }

        # Fallback (should not be hit if event types are complete)
        return {"type": t}

    def _dict_to_event(self, d: dict) -> Event:
        from typing import cast

        from pyvenue.domain.events import (
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
            Instrument,
            OrderId,
            Price,
            Qty,
            Side,
        )

        t = d.pop("type", d.pop("__type__", None))

        if t == "OrderAccepted":
            return OrderAccepted(
                seq=d["seq"],
                ts_ns=d["ts_ns"],
                instrument=Instrument(d["instrument"]),
                account_id=AccountId(d["account_id"]),
                order_id=OrderId(d["order_id"]),
                side=Side(d["side"]),
                price=Price(d["price"]),
                qty=Qty(d["qty"]),
            )
        elif t == "OrderRejected":
            return OrderRejected(
                seq=d["seq"],
                ts_ns=d["ts_ns"],
                instrument=Instrument(d["instrument"]),
                order_id=OrderId(d["order_id"]),
                reason=d["reason"],
            )
        elif t == "OrderCanceled":
            return OrderCanceled(
                seq=d["seq"],
                ts_ns=d["ts_ns"],
                instrument=Instrument(d["instrument"]),
                order_id=OrderId(d["order_id"]),
            )
        elif t == "TradeOccurred":
            return TradeOccurred(
                seq=d["seq"],
                ts_ns=d["ts_ns"],
                instrument=Instrument(d["instrument"]),
                taker_order_id=OrderId(d["taker_order_id"]),
                maker_order_id=OrderId(d["maker_order_id"]),
                price=Price(d["price"]),
                qty=Qty(d["qty"]),
            )
        elif t == "TopOfBookChanged":
            return TopOfBookChanged(
                seq=d["seq"],
                ts_ns=d["ts_ns"],
                instrument=Instrument(d["instrument"]),
                best_bid_ticks=d["best_bid_ticks"],
                best_ask_ticks=d["best_ask_ticks"],
            )
        elif t == "OrderRested":
            return OrderRested(
                seq=d["seq"],
                ts_ns=d["ts_ns"],
                instrument=Instrument(d["instrument"]),
                account_id=AccountId(d["account_id"]),
                order_id=OrderId(d["order_id"]),
                side=Side(d["side"]),
                price=Price(d["price"]),
                qty=Qty(d["qty"]),
            )
        elif t == "OrderExpired":
            return OrderExpired(
                seq=d["seq"],
                ts_ns=d["ts_ns"],
                instrument=Instrument(d["instrument"]),
                order_id=OrderId(d["order_id"]),
                qty=Qty(d["qty"]),
                reason=d["reason"],
            )
        elif t == "FundsCredited":
            return FundsCredited(
                seq=d["seq"],
                ts_ns=d["ts_ns"],
                instrument=Instrument(d["instrument"]),
                account_id=AccountId(d["account_id"]),
                asset=Asset(d["asset"]),
                amount=Qty(d["amount"]),
            )
        elif t == "FundsReserved":
            return FundsReserved(
                seq=d["seq"],
                ts_ns=d["ts_ns"],
                instrument=Instrument(d["instrument"]),
                account_id=AccountId(d["account_id"]),
                asset=Asset(d["asset"]),
                amount=Qty(d["amount"]),
            )
        elif t == "FundsReleased":
            return FundsReleased(
                seq=d["seq"],
                ts_ns=d["ts_ns"],
                instrument=Instrument(d["instrument"]),
                account_id=AccountId(d["account_id"]),
                asset=Asset(d["asset"]),
                amount=Qty(d["amount"]),
            )

        # Fallback dummy event for unknown types
        class DummyEvent:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        return cast(Event, DummyEvent(**d))

    def iter_from(self, seq: int) -> Iterable[Event]:
        if not self.path.exists():
            return
        with open(self.path) as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    d = self._json.loads(line)
                except Exception:
                    continue  # Ignore trailing partial lines
                if "type" not in d and "__type__" not in d:
                    continue
                ev = self._dict_to_event(d)
                if getattr(ev, "seq", 0) >= seq:
                    yield ev

    def append(self, events: list[Event]) -> None:
        with open(self.path, "a") as f:
            for ev in events:
                f.write(self._json.dumps(self._event_to_dict(ev)) + "\n")

    def last_seq(self) -> int:
        seq = 0
        for ev in self.iter_from(0):
            seq = max(seq, ev.seq)
        return seq
