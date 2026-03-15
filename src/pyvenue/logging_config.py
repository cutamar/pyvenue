from __future__ import annotations

import logging
import os
import sys

import structlog


def configure_structlog() -> None:
    level_name = os.getenv("PYVENUE_LOG_LEVEL", "INFO").strip().upper()
    level_name = {"WARN": "WARNING", "FATAL": "CRITICAL"}.get(level_name, level_name)
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=level,
    )

    json_logs = os.getenv("PYVENUE_LOG_JSON", "0").strip().lower() in {
        "1",
        "true",
        "yes",
    }
    renderer = (
        structlog.processors.JSONRenderer()
        if json_logs
        else structlog.dev.ConsoleRenderer()
    )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso", utc=False),
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
