"""Collect radolan data in a single netcdf file per year."""
from typing import *
from typing.io import IO
from pathlib import Path
import tarfile
import rasterio
import numpy as np
from datetime import datetime
import h5netcdf
import cf_units
from concurrent.futures import ProcessPoolExecutor
import logging

logger = logging.getLogger(__name__)


def main():
    base_data_dir = Path(__file__).parents[3] / "data" / "radolan"
    year = "2016"
    collect_to = base_data_dir / "netcdf" / f"{year}_test.nc"
    raw_data_path = base_data_dir / "raw" / year
    collect_to.parent.mkdir(parents=True, exist_ok=True)
    run(collect_to, raw_data_path)


def run(collect_to: Path, tar_file_path: Path) -> None:
    tar_files = sorted(list((tar_file_path).rglob("*.tar")))
    logger.info(f"Start counting frames")
    n_frames = get_number_of_frames(tar_files)
    logger.info(f"Collecting {n_frames} radar frames")
    offset = "minutes since 1970-01-01 00:00:00"
    time_unit = cf_units.Unit(offset, calendar=cf_units.CALENDAR_STANDARD)
    x_size = 900
    y_size = 900

    with h5netcdf.File(collect_to, "w") as f:
        # Dimensions.
        f.dimensions["time"] = n_frames
        f.dimensions["x"] = x_size
        f.dimensions["y"] = y_size

        # Coordinate variables.
        time_var = f.create_variable("time", dimensions=("time",), dtype=int)
        time_var.attrs["units"] = time_unit.name

        f.create_variable("x", dimensions=("x",), data=np.arange(x_size))
        f.create_variable("y", dimensions=("y",), data=np.arange(y_size))

        # Data variables.
        rain_var = f.create_variable(
            "rain",
            dimensions=("time", "y", "x"),
            dtype=int,
            chunks=True,
            compression="lzf",
        )
        rain_var.attrs["units"] = "mm/h"

        rain_var.attrs["_FillValue"] = -1
        start = 0
        end = 0
        for rain, time in collect_year(tar_files):
            end += len(time)
            write_to_netcdf(f, rain, time_unit.date2num(time), start, end)
            start = end


def get_number_of_frames(tar_files: List[Path]) -> int:

    with ProcessPoolExecutor(4) as executor:
        tasks = executor.map(get_number_of_frames_for_single_tar_file, tar_files)

    return sum(tasks)


def get_number_of_frames_for_single_tar_file(tar_file: Path) -> int:
    n_frames = 0
    with tarfile.open(tar_file) as tf_month:
        for day_member in tf_month:
            member = tf_month.extractfile(day_member)
            with tarfile.open(mode="r:gz", fileobj=member) as day_tar_file:
                n_frames += len(day_tar_file.getmembers())

    return n_frames


def collect_year(
    tar_files: List[Path]
) -> Generator[Tuple[List[np.ndarray], List[datetime]], None, None]:
    # Helper to check that all rasters have the same bounding box
    # and the same mask.
    bounding_boxes = set()
    for tar_file_path in tar_files:
        with tarfile.open(tar_file_path) as tf_month:
            day_members = sorted(
                tf_month.getnames(),
                key=lambda x: datetime.strptime(x, "RW-%Y%m%d.tar.gz"),
            )
            for day_member in day_members:
                member = tf_month.extractfile(day_member)
                if member is not None:
                    yield collect_day(member, bounding_boxes)
                else:
                    raise RuntimeError()


def collect_day(
    member: IO[bytes], bounding_boxes: set
) -> Tuple[List[np.ndarray], List[datetime]]:
    arrs = []
    times = []
    with tarfile.open(mode="r:gz", fileobj=member) as day_tar_file:
        hour_members = sorted(
            day_tar_file.getnames(),
            key=lambda x: datetime.strptime(x, "RW_%Y%m%d-%H%M.asc"),
        )

        for hour_asc_file_name in hour_members:
            timestamp = datetime.strptime(hour_asc_file_name, "RW_%Y%m%d-%H%M.asc")
            logger.debug(f"Collecting frame at from {timestamp}")
            times.append(timestamp)
            hour_member = day_tar_file.getmember(hour_asc_file_name)
            with rasterio.open(day_tar_file.extractfile(hour_member)) as raster:
                check_bounding_box(bounding_boxes, raster.bounds)
                arr = raster.read(1)
                arrs.append(arr)

    return arrs, times


def check_bounding_box(
    bboxes: Set[rasterio.coords.BoundingBox], bbox: rasterio.coords.BoundingBox
) -> None:
    bboxes.add(bbox)
    assert len(bboxes) == 1, "Non matching bounding boxes."


def write_to_netcdf(
    f: h5netcdf.File, rain_data: np.ndarray, time_data: np.ndarray, start: int, end: int
) -> None:
    # Coordinate variables.
    f["time"][start:end] = time_data
    # Data variables.
    f["rain"][start:end] = rain_data


if __name__ == "__main__":
    main()
