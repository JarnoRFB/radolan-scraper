"""Extract ascii files from downloaded tar files.

Can be dangerous, because it creates an a very high number of files. 
Prefer ``collect`` to combine the data into a single file.
"""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import re
import tarfile

from tqdm import tqdm


def main():
    base_data_dir = Path(__file__).parents[3] / "data" / "radolan"
    extract_to = base_data_dir / "extracted"
    raw_data = base_data_dir / "raw"
    tar_files = raw_data.rglob("*.tar")

    run_with_progress(lambda x: extract_month(extract_to, x), tar_files)
    clean_up(extract_to)


def run_with_progress(f, items_to_process):
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(
            tqdm(executor.map(f, items_to_process), total=len(items_to_process))
        )
    return results


def extract_month(extract_to: Path, filepath: Path):
    month = re.match(r"RW-20\d\d(\d\d).tar", filepath.name).groups(0)[0]
    new_dir = extract_to / month
    new_dir.mkdir(parents=True, exist_ok=True)

    with tarfile.open(filepath) as tf:
        tf.extractall(new_dir)

    extract_days(new_dir)


def clean_up(dir_path: Path):
    for file_path in dir_path.rglob("*.tar.gz"):
        file_path.unlink()


def extract_days(dir_path: Path):
    tar_files = dir_path.rglob("*.tar.gz")
    for tar_file in tar_files:
        day = re.match(r"RW-20\d\d\d\d(\d\d).tar.gz", tar_file.name).groups(0)[0]
        new_dir = new_dir = tar_file.parent / day
        new_dir.mkdir(parents=True, exist_ok=True)
        with tarfile.open(tar_file, "r:gz") as tf:
            tf.extractall(new_dir)


if __name__ == "__main__":
    main()
