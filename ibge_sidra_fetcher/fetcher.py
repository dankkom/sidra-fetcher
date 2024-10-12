import random
import time
from queue import Queue
from threading import Thread

import httpx

from . import logger
from .storage import write_data


class Fetcher(Thread):
    def __init__(self, q: Queue):
        super().__init__()
        self.daemon = True
        self.q = q

    def run(self):
        client = httpx.Client(timeout=300)
        while True:
            task = self.q.get()
            dest_filepath = task["dest_filepath"]
            url = task["url"]
            try:
                t0 = time.time()
                data = get(url, client)
                t1 = time.time()
                logger.debug(f"Download of {url} took {t1 - t0:.2f} seconds")
                write_data(data, dest_filepath)
            except Exception as e:
                logger.exception("Error %s %s", e, url)
                time.sleep(2 * random.random())
            finally:
                self.q.task_done()
            time.sleep(2 * random.random())


# GET -------------------------------------------------------------------------
def get(
    url: str,
    client: httpx.Client,
) -> bytes:
    logger.info(f"Downloading DATA {url}")
    t0 = time.time()
    data = b""
    with client.stream("GET", url) as r:
        if r.status_code != 200:
            raise ConnectionError(f"Error status code {r.status_code}")
        for chunk in r.iter_bytes():
            data += chunk
    if data is None:
        raise ConnectionError("Data returned is None!")
    t1 = time.time()
    logger.debug(f"Download of {url} took {t1 - t0:.2f} seconds")
    return data
