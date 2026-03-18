from prefect import flow, task
import subprocess
import os

# Flow to run PowerShell scripts for bronze ingestion
#
#

SCRIPTS_DIR = r"C:\DataCycle\Scripts"


@task
def run_powershell(script_name):
    script_path = os.path.join(SCRIPTS_DIR, script_name)

    print(f"Running PowerShell script: {script_path}")

    result = subprocess.run(
        [
            "powershell",
            "-ExecutionPolicy", "Bypass",
            "-File", script_path
        ],
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"{script_name} failed with exit code {result.returncode}")


@flow(name="bronze-ingestion-flow")
def bronze_ingestion_flow():
    run_powershell("eversys_incremental_flat_V6_compat_logs.ps1")


if __name__ == "__main__":
    bronze_ingestion_flow()