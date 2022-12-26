import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def read_json(filepath: Path):
    logger.debug(f"Reading JSON file {filepath}")
    with filepath.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data


def write_json(data: dict | list | bytes, filepath: Path):
    logger.debug(f"Writing JSON file {filepath}")
    if isinstance(data, bytes):
        data = json.loads(data.decode("utf-8"))
    filepath.parent.mkdir(exist_ok=True, parents=True)
    with filepath.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)


def write_data(data: bytes, filepath: Path):
    logger.debug(f"Writing bytes file {filepath}")
    filepath.parent.mkdir(exist_ok=True, parents=True)
    with filepath.open("wb") as f:
        f.write(data)
