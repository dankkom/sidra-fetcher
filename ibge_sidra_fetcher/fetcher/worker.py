import logging
import random
import time
from queue import Queue
from threading import Thread

import httpx

from ..storage.io import write_data
from .get import get

logger = logging.getLogger(__name__)


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
                print("Error", e)
                print("Error", url)
                time.sleep(2 * random.random())
            finally:
                self.q.task_done()
            time.sleep(2 * random.random())
