from __future__ import annotations

import argparse
import cProfile
import pstats

from pyvenue.bench.bench_orderflow import (
    BenchConfig,
    scenario_cancel_heavy,
    scenario_insert_only,
    scenario_replay,
    scenario_sweep,
)
from pyvenue.domain.types import Instrument


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--scenario", choices=["insert", "cancel", "sweep", "replay"], required=True
    )
    ap.add_argument("--n", type=int, default=50_000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--outfile", default="profile.pstats")
    args = ap.parse_args()

    cfg = BenchConfig(
        n=args.n,
        seed=args.seed,
        price_min=90,
        price_max=110,
        qty_min=1,
        qty_max=5,
        instruments=(Instrument("BTC-USD"), Instrument("ETH-USD")),
    )

    prof = cProfile.Profile()
    prof.enable()

    if args.scenario == "insert":
        scenario_insert_only(cfg)
    elif args.scenario == "cancel":
        scenario_cancel_heavy(cfg)
    elif args.scenario == "sweep":
        scenario_sweep(cfg)
    elif args.scenario == "replay":
        scenario_replay(cfg)

    prof.disable()
    prof.dump_stats(args.outfile)

    # Print top 30 by cumulative time
    p = pstats.Stats(args.outfile)
    p.strip_dirs().sort_stats("cumulative").print_stats(30)


if __name__ == "__main__":
    main()
