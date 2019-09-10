"""Combine netcdf files for each year into a single file."""
from typing import *
from typing.io import IO
from pathlib import Path
import numpy as np
import h5netcdf
import cf_units
import logging

logger = logging.getLogger(__name__)


def main():
    base_data_dir = Path(__file__).parents[3] / "data" / "radolan"
    collect_to = base_data_dir / "netcdf"
    collect_to.parent.mkdir(parents=True, exist_ok=True)
    files_to_combine = collect_to.rglob("*.nc")
    run(collect_to, files_to_combine)


def run(combine_to: Path, files_to_combine: Iterable[Path]) -> None:
    total_shape = get_shape(files_to_combine)
    logger.info(f"Creating a dataset of shape {total_shape}")
    offset = "minutes since 1970-01-01 00:00:00"
    time_unit = cf_units.Unit(offset, calendar=cf_units.CALENDAR_STANDARD)
    x_size = total_shape[1]
    y_size = total_shape[2]
    with h5netcdf.File(combine_to, "w") as f:
        # Dimensions.
        f.dimensions["time"] = total_shape[0]
        f.dimensions["y"] = y_size
        f.dimensions["x"] = x_size

        # Coordinate variables.
        time_var = f.create_variable("time", dimensions=("time",), dtype=int)
        time_var.attrs["units"] = time_unit.name

        f.create_variable("x", dimensions=("x",), data=np.arange(x_size))
        f.create_variable("y", dimensions=("y",), data=np.arange(y_size))

        # Data variables.
        rain_var = f.create_variable(
            "rain", dimensions=("time", "y", "x"), dtype=int, compression="lzf"
        )
        rain_var.attrs["units"] = "mm/h"
        rain_var.attrs["_FillValue"] = -1

        start = 0
        end = 0
        for year_netcdf in files_to_combine:
            logger.info(f"Processing {year_netcdf}")
            with h5netcdf.File(year_netcdf, "r") as year_netcdf_file:
                end += len(year_netcdf_file["time"])
                logger.info(f"Writing to [{start}:{end}]")
                write_to_netcdf(f, year_netcdf_file, start, end)
                start = end


def get_shape(files_to_combine: Iterable[Path]) -> Tuple[int, int, int]:
    shapes = []
    for year_netcdf in files_to_combine:
        with h5netcdf.File(year_netcdf, "r") as year_netcdf_file:
            shapes.append(year_netcdf_file["rain"].shape)

    shapes_arr = np.array(shapes)
    assert np.equal.reduce(shapes_arr[:, 1]), "Not all data have equal sizes in y"
    assert np.equal.reduce(shapes_arr[:, 2]), "Not all data have equal sizes in x"
    time_size = shapes_arr[:, 0].sum()
    return (time_size, shapes_arr[0, 1], shapes_arr[0, 2])


def write_to_netcdf(
    f: h5netcdf.File, year_netcdf_file: h5netcdf.File, start: int, end: int
) -> None:
    n_frames = len(year_netcdf_file["time"])
    chunk_size = 100
    n_chunks = n_frames // chunk_size
    chunks = np.linspace(0, n_frames, n_chunks, dtype=int)
    for left, right in zip(chunks[:-1], chunks[1:]):
        logger.debug(f"Processing chunk [{left}:{right}]")
        # Coordinate variables.
        f["time"][start + left : start + right] = year_netcdf_file["time"][left:right]
        # Data variables.
        f["rain"][start + left : start + right] = year_netcdf_file["rain"][left:right]


if __name__ == "__main__":
    main()
