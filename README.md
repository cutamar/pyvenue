# pyvenue

A small, educational (but correctness-first) exchange / matching engine project in Python 3.13.

## Goals
- Deterministic matching engine core (pure logic, no I/O)
- Event-sourced outputs (commands in → events out)
- Correctness first (tests + invariants), then performance (benchmarks + profiling)

## Dev setup (uv)
```bash
# from repo root
uv python install 3.13
uv venv --python 3.13
uv pip install -e ".[dev]"

# checks
ruff check .
ruff format .
black --check .
pyright
pytest -q
```

## Project structure
- `src/pyvenue/` — library code
- `tests/` — pytest tests
- `SPEC.md` — the functional spec we evolve milestone-by-milestone
