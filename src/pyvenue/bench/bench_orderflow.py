from __future__ import annotations

import argparse
import random
import time
from dataclasses import dataclass

from pyvenue.domain.commands import Cancel, PlaceLimit, PlaceMarket
from pyvenue.domain.types import (
    AccountId,
    Instrument,
    OrderId,
    Price,
    Qty,
    Side,
    TimeInForce,
)
from pyvenue.venue import Venue


@dataclass(frozen=True)
class BenchConfig:
    n: int
    seed: int
    price_min: int
    price_max: int
    qty_min: int
    qty_max: int
    instruments: tuple[Instrument, ...]


def _now_ns() -> int:
    # For benchmarking you can use a cheap monotonic timestamp,
    # but keep it deterministic-ish.
    return time.time_ns()


def _mk_limit(
    inst: Instrument,
    acct: AccountId,
    oid: str,
    side: Side,
    price: int,
    qty: int,
    client_ts_ns: int,
) -> PlaceLimit:
    return PlaceLimit(
        instrument=inst,
        account_id=acct,
        order_id=OrderId(oid),
        side=side,
        price=Price(price),
        qty=Qty(qty),
        client_ts_ns=client_ts_ns,
        tif=TimeInForce.GTC,
        post_only=False,
    )


def _mk_cancel(
    inst: Instrument, acct: AccountId, oid: str, client_ts_ns: int
) -> Cancel:
    return Cancel(
        instrument=inst,
        account_id=acct,
        order_id=OrderId(oid),
        client_ts_ns=client_ts_ns,
    )


def _mk_market(
    inst: Instrument, acct: AccountId, oid: str, side: Side, qty: int, client_ts_ns: int
) -> PlaceMarket:
    return PlaceMarket(
        instrument=inst,
        account_id=acct,
        order_id=OrderId(oid),
        side=side,
        qty=Qty(qty),
        client_ts_ns=client_ts_ns,
    )


def _seed_balances(v: Venue, cfg: BenchConfig) -> None:
    """
    Give accounts enough to not trigger rejections.
    Assumes your venue/state provides credit and assets mapping.
    """
    alice = AccountId("alice")
    bob = AccountId("bob")
    fee = AccountId("fee")

    for inst in cfg.instruments:
        base = v.assets.base(inst)
        quote = v.assets.quote(inst)

        # Plenty of money:
        v.state.credit(alice, quote, 10**12)
        v.state.credit(bob, base, 10**9)

        # ensure fee account exists
        v.state.credit(fee, quote, 0)


def _timeit(label: str, fn) -> None:
    t0 = time.perf_counter()
    out = fn()
    t1 = time.perf_counter()
    dt = t1 - t0
    print(f"{label}: {dt:.6f}s")
    return out, dt


def scenario_insert_only(cfg: BenchConfig) -> None:
    rng = random.Random(cfg.seed)
    v = Venue(instruments=list(cfg.instruments))
    _seed_balances(v, cfg)

    alice = AccountId("alice")

    def run():
        for i in range(cfg.n):
            inst = cfg.instruments[i % len(cfg.instruments)]
            side = Side.BUY if (i & 1) == 0 else Side.SELL
            price = rng.randint(cfg.price_min, cfg.price_max)
            qty = rng.randint(cfg.qty_min, cfg.qty_max)
            cmd = _mk_limit(inst, alice, f"o{i}", side, price, qty, client_ts_ns=i)
            v.submit(cmd)

    _, dt = _timeit("insert_only", run)
    print(f"ops/sec: {cfg.n / dt:,.0f}")


def scenario_cancel_heavy(cfg: BenchConfig) -> None:
    rng = random.Random(cfg.seed)
    v = Venue(instruments=list(cfg.instruments))
    _seed_balances(v, cfg)

    alice = AccountId("alice")

    # Insert first (rest-only pattern: prices spaced so they don't cross too often)
    oids: list[tuple[Instrument, str]] = []
    for i in range(cfg.n):
        inst = cfg.instruments[i % len(cfg.instruments)]
        side = Side.BUY
        price = rng.randint(cfg.price_min, cfg.price_max)
        qty = rng.randint(cfg.qty_min, cfg.qty_max)
        oid = f"o{i}"
        v.submit(_mk_limit(inst, alice, oid, side, price, qty, client_ts_ns=i))
        oids.append((inst, oid))

    rng.shuffle(oids)

    def run():
        for j, (inst, oid) in enumerate(oids):
            v.submit(_mk_cancel(inst, alice, oid, client_ts_ns=10_000_000 + j))

    _, dt = _timeit("cancel_heavy", run)
    print(f"ops/sec: {cfg.n / dt:,.0f}")


def scenario_sweep(cfg: BenchConfig) -> None:
    """
    Build a book with asks, then market-buy sweep repeatedly.
    """
    rng = random.Random(cfg.seed)
    v = Venue(instruments=list(cfg.instruments))
    _seed_balances(v, cfg)

    alice = AccountId("alice")
    bob = AccountId("bob")

    inst = cfg.instruments[0]

    # Build asks at increasing prices to simulate depth
    depth = max(1, cfg.n // 10)
    for i in range(depth):
        price = cfg.price_min + i
        qty = rng.randint(cfg.qty_min, cfg.qty_max)
        v.submit(_mk_limit(inst, bob, f"ask{i}", Side.SELL, price, qty, client_ts_ns=i))

    # Sweep with market buys
    sweeps = cfg.n

    def run():
        for i in range(sweeps):
            qty = rng.randint(cfg.qty_min, cfg.qty_max)
            v.submit(
                _mk_market(
                    inst, alice, f"mb{i}", Side.BUY, qty, client_ts_ns=1_000_000 + i
                )
            )

    _, dt = _timeit("sweep_market_buy", run)
    print(f"ops/sec: {sweeps / dt:,.0f}")


def scenario_replay(cfg: BenchConfig) -> None:
    """
    Generate events in-memory, then replay them.
    """
    rng = random.Random(cfg.seed)
    v = Venue(instruments=list(cfg.instruments))
    _seed_balances(v, cfg)

    alice = AccountId("alice")
    bob = AccountId("bob")

    events = []
    for i in range(cfg.n):
        inst = cfg.instruments[i % len(cfg.instruments)]
        # Alternate: bob provides asks, alice takes via market occasionally
        if (i % 5) != 0:
            price = rng.randint(cfg.price_min, cfg.price_max)
            qty = rng.randint(cfg.qty_min, cfg.qty_max)
            events.extend(
                v.submit(
                    _mk_limit(inst, bob, f"m{i}", Side.SELL, price, qty, client_ts_ns=i)
                )
            )
        else:
            qty = rng.randint(cfg.qty_min, cfg.qty_max)
            events.extend(
                v.submit(
                    _mk_market(inst, alice, f"t{i}", Side.BUY, qty, client_ts_ns=i)
                )
            )

    def run():
        Venue.replay(
            instruments=list(cfg.instruments), events=events, rebuild_book=True
        )

    _, dt = _timeit("replay", run)
    print(f"events/sec: {len(events) / dt:,.0f} (events={len(events)})")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--scenario", choices=["insert", "cancel", "sweep", "replay"], required=True
    )
    ap.add_argument("--n", type=int, default=50_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--price-min", type=int, default=90)
    ap.add_argument("--price-max", type=int, default=110)
    ap.add_argument("--qty-min", type=int, default=1)
    ap.add_argument("--qty-max", type=int, default=5)
    ap.add_argument("--instruments", nargs="*", default=["BTC-USD", "ETH-USD"])
    args = ap.parse_args()

    cfg = BenchConfig(
        n=args.n,
        seed=args.seed,
        price_min=args.price_min,
        price_max=args.price_max,
        qty_min=args.qty_min,
        qty_max=args.qty_max,
        instruments=tuple(Instrument(x) for x in args.instruments),
    )

    if args.scenario == "insert":
        scenario_insert_only(cfg)
    elif args.scenario == "cancel":
        scenario_cancel_heavy(cfg)
    elif args.scenario == "sweep":
        scenario_sweep(cfg)
    elif args.scenario == "replay":
        scenario_replay(cfg)
    else:
        raise RuntimeError("unknown scenario")


if __name__ == "__main__":
    main()
