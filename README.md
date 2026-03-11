# Pyvenue

**Disclaimer: This project is strictly for educational and demonstrative purposes. It is not intended, designed, or secured for use as a real-life financial exchange or trading venue.**

## Overview

Pyvenue is a small, highly-optimized, correctness-first exchange / matching engine written in Python 3.13. 

It explores how to build a deterministic, event-sourced matching engine core that accurately processes limit and market orders while managing complex state invariants such as self-trade prevention, time-in-force (IOC, FOK, GTC), partial fill reservations, and strict asset balancing.

## Goals

- **Deterministic core:** The matching engine pure logic relies exclusively on commands and deterministic state transitions, completely isolated from I/O elements.
- **Event-sourced architecture:** Every state mutation produces an immutable domain event (`TradeOccurred`, `OrderAccepted`, `FundsReserved`), allowing the venue to be perfectly reconstructed by replaying the event log.
- **Correctness first:** Enforced through a comprehensive `pytest` suite ensuring zero negative balances, accurate price-time priority, and correct fee calculations.
- **Internal Optimization:** Uses flattened dictionary structures and explicitly-typed serialization to ensure high event throughput with minimal reflection latency.

## Project Structure

The project is structured into distinct layers to separate domain concerns from execution and persistence mechanics:

- `src/pyvenue/domain/` 
  - Contains immutable data structures for the domain.
  - defining types (`Instrument`, `AccountId`, `Side`, `Price`, `Qty`), commands (`PlaceLimit`, `PlaceMarket`, `Cancel`), and the resulting events (`OrderAccepted`, `TradeOccurred`, etc.).
- `src/pyvenue/engine/`
  - The core exchange logic.
  - `engine.py`: Processes commands and dispatches events.
  - `orderbook.py`: The limit order book supporting strict price-time priority matching.
  - `state.py`: Flat, high-performance account and asset state tracking.
- `src/pyvenue/persistence/`
  - Event sourcing implementation. 
  - `event_store.py` manages writing to and replaying from `jsonl` logs using highly optimized, explicit serialization dispatchers.
- `src/pyvenue/infra/`
  - Infrastructure components.
  - Handling ID generation (`Snowflake` style seq/timestamps), fixed clocks for determinism, and structured logging.
- `src/pyvenue/bench/`
  - Benchmarking and profiling tools (`bench_orderflow.py`, `profile_orderflow.py`) to measure the matching engine throughput limit in `events / sec`.
- `tests/`
  - A robust `pytest` suite enforcing trading invariants, order lifecycle states, and partial fill balance limits.

## Dev Setup (uv)

This project uses `uv` for ultra-fast dependency management and virtual environments.

```bash
# from repo root
uv python install 3.13
uv venv --python 3.13
uv sync

# automatic checks on commit
pre-commit install

# manual checks
ruff check .
ruff format .
pyright

# tests
pytest -q
```
