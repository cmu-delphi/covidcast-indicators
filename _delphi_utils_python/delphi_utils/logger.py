"""Structured logger utility for creating JSON logs in Delphi pipelines."""
import logging
import structlog

def get_structured_logger(name = __name__, filename = None):
    """Create a new structlog logger.

    Use the logger returned from this in indicator code using the standard
    wrapper calls, e.g.:

    logger = get_structured_logger(__name__)
    logger.warning("Error", type="Signal too low").

    The output will be rendered as JSON which can easily be consumed by logs
    processors.

    See the structlog documentation for details.

    Parameters
    ---------
    name: Name to use for logger (included in log lines), __name__ from caller
    is a good choice.
    filename: An (optional) file to write log output.
    """
    # Configure the underlying logging configuration
    handlers = [logging.StreamHandler()]
    if filename:
        handlers.append(logging.FileHandler(filename))

    logging.basicConfig(
        format="%(message)s",
        level=logging.INFO,
        handlers=handlers
        )

    # Configure structlog. This uses many of the standard suggestions from
    # the structlog documentation.
    structlog.configure(
        processors=[
            # Filter out log levels we are not tracking.
            structlog.stdlib.filter_by_level,
            # Include logger name in output.
            structlog.stdlib.add_logger_name,
            # Include log level in output.
            structlog.stdlib.add_log_level,
            # Allow formatting into arguments e.g., logger.info("Hello, %s",
            # name)
            structlog.stdlib.PositionalArgumentsFormatter(),
            # Add timestamps.
            structlog.processors.TimeStamper(fmt="iso"),
            # Match support for exception logging in the standard logger.
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            # Decode unicode characters
            structlog.processors.UnicodeDecoder(),
            # Render as JSON
            structlog.processors.JSONRenderer()
        ],
        # Use a dict class for keeping track of data.
        context_class=dict,
        # Use a standard logger for the actual log call.
        logger_factory=structlog.stdlib.LoggerFactory(),
        # Use a standard wrapper class for utilities like log.warning()
        wrapper_class=structlog.stdlib.BoundLogger,
        # Cache the logger
        cache_logger_on_first_use=True,
    )

    return structlog.get_logger(name)
