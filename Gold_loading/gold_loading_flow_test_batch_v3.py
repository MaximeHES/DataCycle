"""
Prefect flow for a first gold test load against a test silver root.
Uses independent state and log folders so it does not pollute production state.
"""

from pathlib import Path
from prefect import flow, task
import os
import subprocess
import sys

PYTHON_EXE = sys.executable
LOADER_DIR = Path(__file__).resolve().parent
TEST_SILVER_ROOT = Path(r"C:\Users\Maxime\Downloads\SilverBatch2\SilverBatch2")
TEST_STATE_DIR = TEST_SILVER_ROOT / "_state" / "gold_test_v3"
TEST_LOG_DIR = TEST_SILVER_ROOT / "_logs" / "Eversys_Gold_Test_V3"


def _build_env() -> dict:
    env = os.environ.copy()
    env["EVERSYS_SILVER_ROOT"] = str(TEST_SILVER_ROOT)
    env["EVERSYS_GOLD_STATE_DIR"] = str(TEST_STATE_DIR)
    env["EVERSYS_GOLD_LOG_DIR"] = str(TEST_LOG_DIR)
    return env


def _run(script_name: str) -> None:
    script_path = LOADER_DIR / script_name
    print(f"Running gold loader: {script_path}")
    print(f"  Silver root : {TEST_SILVER_ROOT}")
    print(f"  State dir   : {TEST_STATE_DIR}")
    print(f"  Log dir     : {TEST_LOG_DIR}")
    result = subprocess.run([PYTHON_EXE, str(script_path)], capture_output=True, text=True, env=_build_env())
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr)
        raise RuntimeError(f"{script_name} failed with exit code {result.returncode}")


#@task(name="load-production-gold-test-v3")
#def load_production():
    #_run("load_production_gold_v3.py")


@task(name="load-cleaning-gold-test-v3")
def load_cleaning():
    _run("load_cleaning_gold_v3.py")


@task(name="load-rinse-gold-test-v3")
def load_rinse():
    _run("load_rinse_gold_v3.py")


@task(name="load-alerts-gold-test-v3")
def load_alerts():
    _run("load_alerts_gold_v3.py")


@flow(name="gold-loading-flow-test-batch-v3")
def gold_loading_flow_test_batch_v3():
    expected_dirs = [
        TEST_SILVER_ROOT / "Product_History",
        TEST_SILVER_ROOT / "Cleaning_History",
        TEST_SILVER_ROOT / "Rinse_History",
        TEST_SILVER_ROOT / "Info_Message_History",
    ]
    missing = [str(p) for p in expected_dirs if not p.exists()]
    if missing:
        raise FileNotFoundError("Missing expected test batch folders: " + ", ".join(missing))

    #load_production()
    load_cleaning()
    load_rinse()
    load_alerts()


if __name__ == "__main__":
    gold_loading_flow_test_batch_v3()
