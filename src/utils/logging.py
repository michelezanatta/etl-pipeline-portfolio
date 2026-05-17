from __future__ import annotations

import logging
import time
from functools import wraps
from typing import Callable, TypeVar, ParamSpec

P = ParamSpec("P")
R = TypeVar("R")

def configure_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

def log_step(logger: logging.Logger | None = None):
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        log = logger or logging.getLogger(func.__module__)

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            start = time.time()
            log.info("Starting %s", func.__name__)
            try:
                result = func(*args, **kwargs)
                elapsed = time.time() - start
                log.info("Finished %s in %.2fs", func.__name__, elapsed)
                return result
            except Exception:
                log.exception("Failed %s", func.__name__)
                raise

        return wrapper
    return decorator