import logging


def setup_logging():
    """Configures the root logger with a file handler and console handler."""

    # 1. Define the log format
    formatter = logging.Formatter(
        # Format: Time - Module - Level - Message [Function:Line]
        "%(asctime)s : %(name)s : %(levelname)s - %(message)s [%(funcName)s:%(lineno)d]",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 2. Define Handlers (where the logs go)
    # Console Handler for real-time output
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # 3. Configure the Root Logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)  # Set the minimum level to log

    # Add handlers to the root logger
    root_logger.addHandler(console_handler)

    # Optional: Suppress noisy third-party library logs (e.g., Kafka, urllib3)
    logging.getLogger("kafka").setLevel(logging.WARNING)
