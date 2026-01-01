from __future__ import annotations

from pyvenue.domain.commands import Cancel, PlaceLimit
from pyvenue.domain.types import Instrument, OrderId, Price, Qty, Side
from pyvenue.engine import Engine


def main() -> None:
    engine = Engine()
    cmds = [
        PlaceLimit(
            instrument=Instrument("BTC-USD"),
            order_id=OrderId("o1"),
            side=Side.BUY,
            price=Price(100),
            qty=Qty(5),
            client_ts_ns=1,
        ),
        Cancel(
            instrument=Instrument("BTC-USD"), order_id=OrderId("o1"), client_ts_ns=2
        ),
    ]
    for cmd in cmds:
        engine.handle(cmd)


if __name__ == "__main__":
    main()
