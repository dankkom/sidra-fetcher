import configparser
import logging
from logging import handlers
from pathlib import Path

_config = configparser.ConfigParser()
_config.read(Path("config.ini"))

USER_AGENT = _config["DEFAULT"]["USER_AGENT"]
HTTP_HEADERS = {
    "User-Agent": USER_AGENT,
}
TIMEOUT = int(_config["DEFAULT"]["TIMEOUT"])
SIDRA_API_LIMIT = 100_000


def setup_logging(logger_name: str, log_filepath: Path | str) -> logging.Logger:
    logger = logging.getLogger(logger_name)
    logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    log_formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File log
    filehandler = handlers.RotatingFileHandler(
        filename=log_filepath,
        mode="a",
        maxBytes=50 * 2**20,
        backupCount=100,
    )
    filehandler.setFormatter(log_formatter)
    filehandler.setLevel(logging.INFO)
    logger.addHandler(filehandler)

    # Console log
    streamhandler = logging.StreamHandler()
    streamhandler.setFormatter(log_formatter)
    streamhandler.setLevel(logging.INFO)
    logger.addHandler(streamhandler)

    return logger
