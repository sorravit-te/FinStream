"""Shared logging configuration for the FinStream package."""

import logging


_PACKAGE_LOGGER_NAME = "finstream"
_HANDLER_NAME = "finstream_stream"
_LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"


def _resolve_log_level(level: str | int) -> int:
    if isinstance(level, int):
        return level

    if isinstance(level, str):
        normalized_level = level.strip().upper()
        resolved_level = logging.getLevelNamesMapping().get(normalized_level)
        if resolved_level is not None:
            return resolved_level
        raise ValueError(f"Invalid log level: {level!r}")

    raise TypeError("Log level must be a string or integer")


def configure_logging(level: str | int = "INFO") -> logging.Logger:
    """Configure and return the FinStream package logger."""
    resolved_level = _resolve_log_level(level)
    logger = logging.getLogger(_PACKAGE_LOGGER_NAME)
    logger.setLevel(resolved_level)
    logger.propagate = False

    handler = next(
        (
            existing_handler
            for existing_handler in logger.handlers
            if existing_handler.get_name() == _HANDLER_NAME
        ),
        None,
    )
    if handler is None:
        handler = logging.StreamHandler()
        handler.set_name(_HANDLER_NAME)
        logger.addHandler(handler)

    handler.setLevel(resolved_level)
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))

    return logger
