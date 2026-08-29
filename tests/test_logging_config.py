import logging

import pytest

from finstream.logging_config import configure_logging


_HANDLER_NAME = "finstream_stream"


@pytest.fixture
def restore_finstream_logger_state():
    logger = logging.getLogger("finstream")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    logger.handlers.clear()

    yield logger

    logger.handlers[:] = original_handlers
    logger.setLevel(original_level)
    logger.propagate = original_propagate


def _finstream_handlers(logger: logging.Logger) -> list[logging.Handler]:
    return [handler for handler in logger.handlers if handler.get_name() == _HANDLER_NAME]


def test_configures_finstream_logger(
    restore_finstream_logger_state: logging.Logger,
) -> None:
    logger = configure_logging(" WARNING ")
    handlers = _finstream_handlers(logger)

    assert logger is logging.getLogger("finstream")
    assert logger.level == logging.WARNING
    assert logger.propagate is False
    assert len(handlers) == 1
    assert handlers[0].level == logging.WARNING


def test_repeated_configuration_reuses_finstream_handler(
    restore_finstream_logger_state: logging.Logger,
) -> None:
    logger = configure_logging("INFO")
    original_handler = _finstream_handlers(logger)[0]

    configured_logger = configure_logging(logging.DEBUG)
    handlers = _finstream_handlers(configured_logger)

    assert configured_logger.level == logging.DEBUG
    assert handlers == [original_handler]
    assert handlers[0].level == logging.DEBUG


def test_invalid_log_level_raises_value_error(
    restore_finstream_logger_state: logging.Logger,
) -> None:
    with pytest.raises(ValueError, match="Invalid log level"):
        configure_logging("not-a-level")
