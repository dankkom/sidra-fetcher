import configparser
import logging.config
import os
from pathlib import Path

CONFIG_DIR = Path()
if not CONFIG_DIR:
    raise FileNotFoundError("No configuration directory found in environment variables")
CONFIG_DIR = Path(CONFIG_DIR)
DATA_DIR = os.getenv("DATA_DIR")
if not DATA_DIR:
    raise FileNotFoundError("No data directory found in environment variables")
DATA_DIR = Path(DATA_DIR) / "raw" / "ibge" / "sidra"
DATA_DIR.mkdir(exist_ok=True, parents=True)

_config = configparser.ConfigParser()
_config.read(CONFIG_DIR / "config.ini")

USER_AGENT = _config["DEFAULT"]["USER_AGENT"]
HTTP_HEADERS = {
    "User-Agent": USER_AGENT,
}
TIMEOUT = int(_config["DEFAULT"]["TIMEOUT"])

_logging_config_filepath = CONFIG_DIR / "logging.ini"
if not _logging_config_filepath.exists():
    raise FileNotFoundError("No logging configuration file exists")
logging.config.fileConfig(_logging_config_filepath)
