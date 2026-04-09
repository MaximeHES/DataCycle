"""
Prefect flow: runs all silver -> gold loaders in sequence.
Each loader is isolated and idempotent at both file and row level.
"""

from pathlib import Path
from prefect import flow, task
import subprocess
import sys

PYTHON_EXE = sys.executable
LOADER_DIR = Path(__file__).resolve().parent


def _run(script_name: str) -> None:
    script_path = LOADER_DIR / script_name
    print(f"Running gold loader: {script_path}")
    result = subprocess.run([PYTHON_EXE, str(script_path)], capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr)
        raise RuntimeError(f"{script_name} failed with exit code {result.returncode}")


@task(name="load-production-gold-v3")
def load_production():
    _run("load_production_gold_v3.py")


@task(name="load-cleaning-gold-v3")
def load_cleaning():
    _run("load_cleaning_gold_v3.py")


@task(name="load-rinse-gold-v3")
def load_rinse():
    _run("load_rinse_gold_v3.py")


@task(name="load-alerts-gold-v3")
def load_alerts():
    _run("load_alerts_gold_v3.py")


@flow(name="gold-loading-flow-v3")
def gold_loading_flow_v3():
    load_production()
    load_cleaning()
    load_rinse()
    load_alerts()


if __name__ == "__main__":
    gold_loading_flow_v3()
