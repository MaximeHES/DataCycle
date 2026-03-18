from prefect import flow, task
import subprocess
import sys
import os

# Python executable
PYTHON_EXE = sys.executable

# Folder where the scripts are located
BASE_DIR = r"C:\DataCycle\ETL\Orchestrator"


@task
def run_cleaner(script_name):

    script_path = os.path.join(BASE_DIR, script_name)

    print(f"Running {script_name}")

    result = subprocess.run(
        [PYTHON_EXE, script_path],
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"{script_name} failed")


@flow(name="eversys-etl-pipeline")
def eversys_pipeline():

    run_cleaner("clean_product_history.py")
    run_cleaner("clean_rinse_history.py")
    run_cleaner("clean_info_message_history.py")
    run_cleaner("clean_cleaning_history.py")


if __name__ == "__main__":
    eversys_pipeline()