from __future__ import annotations

import logging
import os

import structlog


def configure_structlog() -> None:
    level_name = os.getenv("PYVENUE_LOG_LEVEL", "INFO").strip().upper()
    level_name = {"WARN": "WARNING", "FATAL": "CRITICAL"}.get(level_name, level_name)
    level = getattr(logging, level_name, logging.INFO)

    json_logs = os.getenv("PYVENUE_LOG_JSON", "0").strip().lower() in {"1", "true", "yes"}
    renderer = structlog.processors.JSONRenderer() if json_logs else structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=False),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )