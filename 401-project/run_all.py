"""Run the complete NYC taxi-demand pipeline with fail-fast behavior."""

import argparse
import os
from pathlib import Path
import subprocess
import sys


SCRIPT_DIR = Path(__file__).resolve().parent
REPOSITORY_ROOT = SCRIPT_DIR.parent
SOURCE_DIR = REPOSITORY_ROOT / "src"


def configure_environment() -> None:
    """Configure Python/Spark without machine-specific absolute paths."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    try:
        import pyspark
    except ImportError as exc:
        raise SystemExit(
            "PySpark is not installed. Run: python -m pip install -r ../requirements.txt"
        ) from exc

    os.environ.setdefault("SPARK_HOME", str(Path(pyspark.__file__).resolve().parent))

    # Hadoop binaries are only needed by some Windows installations. Respect an
    # existing HADOOP_HOME; otherwise use the bundled compatibility folder.
    bundled_hadoop = REPOSITORY_ROOT / "hadoop"
    if os.name == "nt" and "HADOOP_HOME" not in os.environ and bundled_hadoop.exists():
        os.environ["HADOOP_HOME"] = str(bundled_hadoop)
        os.environ["PATH"] = str(bundled_hadoop / "bin") + os.pathsep + os.environ["PATH"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download the configured NYC TLC data before running the pipeline.",
    )
    parser.add_argument(
        "--months",
        default="01,02,03,04,05,06,07,08,09,10,11,12",
        help="İndirilecek aylar (varsayılan: 2019 yılının 12 ayı).",
    )
    parser.add_argument(
        "--skip-visualization",
        action="store_true",
        help="Stop after batch inference.",
    )
    return parser.parse_args()


def run_step(label: str, command: list[str]) -> None:
    print(f"\n{'=' * 64}\n{label}\n{'=' * 64}", flush=True)
    subprocess.run(command, cwd=SCRIPT_DIR, check=True)


def main() -> None:
    args = parse_args()
    configure_environment()

    steps: list[tuple[str, list[str]]] = []
    if args.download:
        steps.append(
            (
                "Downloading NYC TLC source data",
                [sys.executable, str(SCRIPT_DIR / "veri_indirme.py"), "--months", args.months],
            )
        )

    pipeline_scripts = [
        "01_data_ingestion.py",
        "02_feature_engineering.py",
        "03_model_training.py",
        "04_inference.py",
    ]
    if not args.skip_visualization:
        pipeline_scripts.append("05_visualization.py")

    steps.extend(
        (f"Running {script_name}", [sys.executable, str(SOURCE_DIR / script_name)])
        for script_name in pipeline_scripts
    )

    try:
        for label, command in steps:
            run_step(label, command)
    except subprocess.CalledProcessError as exc:
        raise SystemExit(f"Pipeline stopped: {exc.cmd[-1]} exited with code {exc.returncode}") from exc

    print("\nPipeline completed successfully.")


if __name__ == "__main__":
    main()
