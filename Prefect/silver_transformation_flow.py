from prefect import flow, task
import subprocess
import sys
import os


PYTHON_EXE = sys.executable
CLEANER_DIR = r"C:\DataCycle\ETL\Orchestrator"


@task
def run_cleaner(script_name):
    script_path = os.path.join(CLEANER_DIR, script_name)

    print(f"Running cleaner: {script_path}")

    result = subprocess.run(
        [PYTHON_EXE, script_path],
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"{script_name} failed with exit code {result.returncode}")


@flow(name="silver-transformation-flow")
def silver_transformation_flow():
    run_cleaner("clean_product_history.py")
    run_cleaner("clean_rinse_history.py")
    run_cleaner("clean_info_message_history.py")
    run_cleaner("clean_cleaning_history.py")


if __name__ == "__main__":
    silver_transformation_flow()