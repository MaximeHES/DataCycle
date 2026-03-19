import subprocess
import smtplib
import sys
from pathlib import Path
from datetime import datetime
from email.mime.text import MIMEText

# ============================================================
# CONFIG
# ============================================================

BASE_DIR = Path(r"C:\DataCycle\ETL\Orchestrator")

# Better than sys.executable for Task Scheduler:
PYTHON_EXEC = r"python"

SCRIPTS = [
    "clean_product_history.py",
    "clean_rinse_history.py",
    "clean_info_message_history.py",
    "clean_cleaning_history.py"
]

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

SMTP_USERNAME = "python.projectmonitoring@gmail.com"
SMTP_PASSWORD = "YOUR_APP_PASSWORD"

EMAIL_FROM = "python.projectmonitoring@gmail.com"
EMAIL_TO = ["python.projectmonitoring@gmail.com"]


# ============================================================
# EMAIL
# ============================================================

def send_email(subject, body):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(EMAIL_TO)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())


# ============================================================
# MAIN
# ============================================================

def main():
    start_time = datetime.now()
    print(f"ETL started at {start_time:%Y-%m-%d %H:%M:%S}")

    processes = []

    # Start all scripts in parallel
    for script in SCRIPTS:
        script_path = BASE_DIR / script
        print(f"Launching {script} ...")

        process = subprocess.Popen(
            [PYTHON_EXEC, str(script_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(BASE_DIR)
        )

        processes.append({
            "script": script,
            "process": process
        })

    # Wait for all to finish and collect results
    results = []
    failure = False

    for item in processes:
        script = item["script"]
        process = item["process"]

        stdout, stderr = process.communicate()

        result = {
            "script": script,
            "returncode": process.returncode,
            "stdout": stdout,
            "stderr": stderr
        }
        results.append(result)

        if process.returncode == 0:
            print(f"{script} : SUCCESS")
        else:
            print(f"{script} : FAILED")
            failure = True

    report_lines = []
    for r in results:
        status = "SUCCESS" if r["returncode"] == 0 else "FAILED"
        report_lines.append(f"{r['script']} : {status}")

        if r["returncode"] != 0:
            report_lines.append("STDERR:")
            report_lines.append(r["stderr"][:2000] if r["stderr"] else "(empty)")
            report_lines.append("STDOUT:")
            report_lines.append(r["stdout"][:2000] if r["stdout"] else "(empty)")
            report_lines.append("")

    report = "\n".join(report_lines)

    print("\nExecution summary:")
    print(report)

    if failure:
        subject = "Eversys ETL FAILURE"
        body = f"""ETL Execution Failed

Time: {datetime.now():%Y-%m-%d %H:%M:%S}

Results:
{report}
"""
        send_email(subject, body)
        sys.exit(1)

    print("\nAll ETL jobs completed successfully")
    sys.exit(0)


if __name__ == "__main__":
    main()