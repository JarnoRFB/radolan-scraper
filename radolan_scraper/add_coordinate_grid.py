"""Add the multidimensional coordinates to the netcdf file."""
from pathlib import Path
from typing import *

import h5netcdf
import numpy as np


def main():
    base_data_dir = Path(__file__).parents[3] / "data" / "radolan"
    metadata_dir = Path(__file__).parents[1] / "metadata"
    add_grid_to = base_data_dir / "netcdf" / "combined.nc"
    run(
        add_grid_to, metadata_dir / "phi_center.txt", metadata_dir / "lambda_center.txt"
    )


def run(
    add_grid_to: Path, latitude_definitions: Path, longitude_definitions: Path
) -> None:

    with h5netcdf.File(add_grid_to, "a") as f:

        try:
            xc_var = f.create_variable(
                "xc",
                dimensions=("y", "x"),
                data=parse_longitude_definitions(longitude_definitions).flatten(),
            )
            xc_var.attrs["long_name"] = "longitude of grid cell center"
            xc_var.attrs["units"] = "degrees_east"
            xc_var.attrs["bounds"] = "xv"
        except ValueError:
            xc_var = f["xc"]
            xc_var[...] = parse_longitude_definitions(longitude_definitions)

        try:
            yc_var = f.create_variable(
                "yc",
                dimensions=("y", "x"),
                data=parse_latitude_definitions(latitude_definitions).flatten(),
            )
            yc_var.attrs["long_name"] = "latitude of grid cell center"
            yc_var.attrs["units"] = "degrees_north"
            yc_var.attrs["bounds"] = "yv"
        except ValueError:
            yc_var = f["yc"]
            yc_var[...] = parse_latitude_definitions(latitude_definitions)

        rain_var = f["rain"]
        rain_var.attrs["coordinates"] = "yc xc"


def parse_longitude_definitions(coord_definition_path) -> np.array:
    import itertools

    with open(coord_definition_path) as f:
        return np.fromiter(
            map(
                float,
                itertools.chain.from_iterable(
                    chunk_str(line, 8) for line in f.readlines()
                ),
            ),
            float,
        ).reshape(900, 900)


def parse_latitude_definitions(coord_definition_path) -> np.array:
    return parse_longitude_definitions(coord_definition_path)[::-1]


def chunk_str(iterable: Iterable, n: int) -> Iterable[str]:
    """Collect data into fixed-length chunks or blocks"""
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return map(lambda x: "".join(map(str, x)), zip(*args))


if __name__ == "__main__":
    main()
