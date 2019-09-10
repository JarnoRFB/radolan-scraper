from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import logging
import logging.config
import os
from pathlib import Path

import luigi
import yaml
from dotenv import load_dotenv

import add_coordinate_grid
import collect
import combine
import extract
import logging_example
import scrape


def setup_logging(default_path="logging.yaml"):
    path = os.getenv("LOG_CFG", default_path)
    if os.path.exists(path):
        with open(path) as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        raise ValueError("Logging config not found")


def get_base_data_dir():
    base_dir = os.getenv("BASE_DATA_DIR")
    if base_dir is not None:
        return Path(base_dir)
    else:
        raise ValueError("BASE_DATA_DIR not configured in environment.")


class ScrapeRadolan(luigi.Task):
    year = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(get_base_data_dir() / "raw" / str(self.year))

    def run(self):
        data_path = Path(self.output().path)
        data_path.mkdir(parents=True, exist_ok=True)
        scrape.run_in_loop(data_path, [self.year])


class ExtractTarFiles(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return ScrapeRadolan(self.year)

    def output(self):
        return luigi.LocalTarget(get_base_data_dir() / "extracted" / str(self.year))

    def run(self):
        extract_to = Path(self.output().path)
        tar_files = list(Path(self.input().path).rglob("*.tar"))
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [
                executor.submit(
                    lambda x: extract.extract_month(extract_to, x), tar_file
                )
                for tar_file in tar_files
            ]
            for i, _ in enumerate(concurrent.futures.as_completed(futures)):
                progress_percentage = i / len(tar_files)
                self.set_progress_percentage(progress_percentage)
                self.set_status_message("Progress: %d / 100" % progress_percentage)
        extract.clean_up(extract_to)


class CreateNetCDFFromTarFiles(luigi.Task):
    year = luigi.Parameter()

    def requires(self):
        return ScrapeRadolan(self.year)

    def output(self):
        return luigi.LocalTarget(get_base_data_dir() / "netcdf" / f"{self.year}.nc")

    def run(self):
        collect_to = Path(self.output().path)
        collect_to.parent.mkdir(parents=True, exist_ok=True)
        collect.run(collect_to, Path(self.input().path))


class CombineNetCDFFiles(luigi.Task):
    years = luigi.ListParameter()

    def requires(self):
        return [CreateNetCDFFromTarFiles(year) for year in self.years]

    def output(self):
        return luigi.LocalTarget(get_base_data_dir() / "netcdf" / f"combined.nc")

    def run(self):
        combine_to = Path(self.output().path)
        combine_to.parent.mkdir(parents=True, exist_ok=True)
        combine.run(combine_to, [Path(input_.path) for input_ in self.input()])


class AddCoordinateGridToCombinedNetCDFFile(luigi.Task):
    years = luigi.ListParameter()

    def requires(self):
        return CombineNetCDFFiles(years)

    def output(self):
        return luigi.LocalTarget(get_base_data_dir() / "netcdf" / f"_grid_added.luigi")

    def run(self):
        metadata_dir = Path(__file__).parents[1] / "metadata"
        latitude_definitions = metadata_dir / "phi_center.txt"
        longitude_definitions = metadata_dir / "lambda_center.txt"

        add_grid_to = Path(self.input().path)
        add_coordinate_grid.run(
            add_grid_to, latitude_definitions, longitude_definitions
        )
        # Write a sentinel file to mark the task as done.
        with self.output().open("w") as _:
            pass


if __name__ == "__main__":
    load_dotenv()
    setup_logging()
    years = list((range(2005, 2019)))
    tasks = [AddCoordinateGridToCombinedNetCDFFile(years)]
    luigi.build(tasks, local_scheduler=True, workers=1)
