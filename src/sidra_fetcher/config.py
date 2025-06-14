import configparser
from pathlib import Path

_config = configparser.ConfigParser()
_config.read(Path("config.ini"))

USER_AGENT = _config["DEFAULT"]["USER_AGENT"]
HTTP_HEADERS = {
    "User-Agent": USER_AGENT,
}
TIMEOUT = int(_config["DEFAULT"]["TIMEOUT"])
SIDRA_API_LIMIT = 100_000
