"""Download NYC TLC Yellow Taxi Parquet files and the taxi-zone lookup."""

import argparse
from pathlib import Path
import urllib.request


PROJECT_DIR = Path(__file__).resolve().parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2019)
    parser.add_argument(
        "--months",
        default="01,02,03,04,05,06,07,08,09,10,11,12",
        help="Virgülle ayrılmış ay numaraları; varsayılan 2019 yılının 12 ayıdır.",
    )
    parser.add_argument("--force", action="store_true", help="Overwrite files already present.")
    return parser.parse_args()


def download(url: str, destination: Path, force: bool) -> None:
    if destination.exists() and destination.stat().st_size > 0 and not force:
        print(f"Skipping existing file: {destination.name}")
        return

    temporary = destination.with_suffix(destination.suffix + ".part")
    print(f"Downloading: {url}")
    try:
        urllib.request.urlretrieve(url, temporary)
        temporary.replace(destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    print(f"Saved: {destination}")


def main() -> None:
    args = parse_args()
    months = [item.strip().zfill(2) for item in args.months.split(",") if item.strip()]
    invalid = [month for month in months if not month.isdigit() or not 1 <= int(month) <= 12]
    if not months or invalid:
        raise SystemExit(f"Invalid --months value. Invalid entries: {invalid or 'none supplied'}")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for month_value in months:
        file_name = f"yellow_tripdata_{args.year}-{month_value}.parquet"
        download(f"{BASE_URL}/trip-data/{file_name}", RAW_DIR / file_name, args.force)

    download(
        f"{BASE_URL}/misc/taxi_zone_lookup.csv",
        RAW_DIR / "taxi_zone_lookup.csv",
        args.force,
    )


if __name__ == "__main__":
    main()
