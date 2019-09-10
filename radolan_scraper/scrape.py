"""Downloads all the data from https://opendata.dwd.de/climate_environment/CDC/grids_germany/hourly/radolan/historical/asc/

Downloading takes a few minutes and consumes 5G disk space.
"""
import asyncio
from contextlib import closing
import logging
from pathlib import Path
import re
from typing import *

import aiohttp
from yarl import URL

logger = logging.getLogger(__name__)


def main():
    base_data_dir = Path(__file__).parents[3] / "data" / "radolan"
    data_path = base_data_dir / "raw"
    data_path.mkdir(parents=True, exist_ok=True)
    logger.info("Start downlaoding radolan data")
    with closing(asyncio.get_event_loop()) as loop:
        loop.run_until_complete(run(loop, data_path, range(2005, 2019), 20))

    logger.info("Finished downlaoding radolan data")


def run_in_loop(data_path, years, n_consumers=20):
    with closing(asyncio.get_event_loop()) as loop:
        loop.run_until_complete(run(loop, data_path, years, n_consumers))


async def run(loop, data_path, years, n_consumers=20):
    base_url = URL(
        "https://opendata.dwd.de/climate_environment/CDC/grids_germany/hourly/radolan/historical/asc/"
    )
    urls = [base_url / str(year) for year in years]
    queue = asyncio.Queue()
    # schedule the consumer
    consumers = []
    for _ in range(n_consumers):
        consumer = loop.create_task(consume(queue, data_path))
        consumers.append(consumer)
    # run the producer and wait for completion
    await produce(queue, urls)
    # wait until the consumer has processed all items
    await queue.join()
    # the consumer is still awaiting for an item, cancel it
    for consumer in consumers:
        consumer.cancel()


async def consume(queue, data_path: Path):
    while True:
        item = await queue.get()

        logger.debug(f"Downloading {item}")
        await download_one(data_path, item)

        queue.task_done()


async def produce(queue, urls: List[URL]):
    for url in urls:
        logger.info(f"Extracting links for {url.name}")
        async with aiohttp.ClientSession() as session:
            text = await fetch(session, url)
        filenames = extract_filenames(text)
        for filename in filenames:
            await queue.put(url / filename)


def extract_filenames(table: str) -> List[str]:
    return re.findall(r">(.+\.tar)", table)


async def download_one(data_path: Path, url: URL):
    filename = get_filename(data_path, url)
    filename.parent.mkdir(parents=True, exist_ok=True)
    async with aiohttp.ClientSession() as session:
        await stream(session, url, filename)


async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()


async def stream(session, url, filename, chunk_size=20):
    async with session.get(url) as response:
        with open(filename, "wb") as fd:
            while True:
                chunk = await response.content.read(chunk_size)
                if not chunk:
                    break
                fd.write(chunk)


def get_filename(data_path: Path, url: URL) -> Path:
    year, name = url.parts[-2:]
    return data_path / year / name


if __name__ == "__main__":
    main()
