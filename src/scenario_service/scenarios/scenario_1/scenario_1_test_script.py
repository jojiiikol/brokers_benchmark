import logging
import time

logger = logging.getLogger(__name__)


def scenario():
    N = 100
    SLEEP_TIME_SEC = 1

    time_start = time.time()
    for i in range(0, N):
        logger.warning(f"{i}: {time.time()-time_start}")
        time_start = time.time()
        time.sleep(SLEEP_TIME_SEC)

if __name__ == '__main__':
    scenario()