import configparser
import logging
import os
from logging import handlers
from pathlib import Path

CONFIG_DIR = Path()
if not CONFIG_DIR:
    raise FileNotFoundError("No configuration directory found in environment variables")
CONFIG_DIR = Path(CONFIG_DIR)
DATA_DIR = os.getenv("DATA_DIR")
if not DATA_DIR:
    raise FileNotFoundError("No data directory found in environment variables")
DATA_DIR = Path(DATA_DIR) / "raw" / "ibge-sidra"
DATA_DIR.mkdir(exist_ok=True, parents=True)

_config = configparser.ConfigParser()
_config.read(CONFIG_DIR / "config.ini")

USER_AGENT = _config["DEFAULT"]["USER_AGENT"]
HTTP_HEADERS = {
    "User-Agent": USER_AGENT,
}
TIMEOUT = int(_config["DEFAULT"]["TIMEOUT"])


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
    streamhandler.setLevel(logging.DEBUG)
    logger.addHandler(streamhandler)

    return logger
