import re
from pathlib import Path
from datetime import datetime

import pandas as pd

# ============================================================
# CONFIG
# ============================================================

BRONZE_DIR = Path(r"C:\RawData\Eversys\Info_Message_History")
SILVER_DIR = Path(r"C:\RawData\Eversys_Cleaned\Info_Message_History")


# ============================================================
# HELPERS
# ============================================================

def extract_file_timestamp(filename: str) -> datetime | None:
    match = re.match(r"(\d{4}-\d{2}-\d{2})_(\d{2})_(\d{2})_(\d{2})-", filename)
    if not match:
        return None

    try:
        return datetime.strptime(
            f"{match.group(1)} {match.group(2)}:{match.group(3)}:{match.group(4)}",
            "%Y-%m-%d %H:%M:%S"
        )
    except ValueError:
        return None


def normalize_datetime_series(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()

    dt_iso = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d %H:%M:%S")
    dt_eu = pd.to_datetime(s, errors="coerce", format="%d/%m/%Y %H:%M:%S")

    result = dt_iso.copy()
    result[result.isna()] = dt_eu[result.isna()]

    return result.dt.strftime("%Y-%m-%d %H:%M:%S")


def clean_common_strings(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({
                "": pd.NA,
                "nan": pd.NA,
                "None": pd.NA,
                "NULL": pd.NA,
                "null": pd.NA
            })
    return df


def build_output_path(source_file: Path) -> Path:
    file_ts = extract_file_timestamp(source_file.name)
    if not file_ts:
        raise ValueError(f"Cannot extract timestamp from filename: {source_file.name}")

    target_dir = (
        SILVER_DIR
        / file_ts.strftime("%Y")
        / file_ts.strftime("%m")
        / file_ts.strftime("%d")
    )
    target_dir.mkdir(parents=True, exist_ok=True)

    return target_dir / f"{source_file.stem}_CLEANED.csv"


def add_metadata_columns(df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
    file_ts = extract_file_timestamp(source_file.name)

    df["source_file"] = source_file.name
    df["file_timestamp"] = file_ts.strftime("%Y-%m-%d %H:%M:%S") if file_ts else pd.NA
    df["ingestion_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return df


# ============================================================
# CLEANING
# ============================================================

def clean_info_message_file(file_path: Path) -> pd.DataFrame:
    df = pd.read_csv(
        file_path,
        sep=";",
        encoding="utf-8-sig",
        dtype=str,
        keep_default_na=False
    )

    # Clean column names
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()

    # Clean strings
    df = clean_common_strings(df)

    # Validate required columns
    required_columns = ["machine_id", "timestamp", "number"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Normalize timestamp
    df["timestamp"] = normalize_datetime_series(df["timestamp"])

    # Convert numeric columns
    if "machine_id" in df.columns:
        df["machine_id"] = pd.to_numeric(df["machine_id"], errors="coerce")

    if "type_number" in df.columns:
        df["type_number"] = pd.to_numeric(df["type_number"], errors="coerce")

    # Remove exact duplicates
    df = df.drop_duplicates()

    # Split message code like S-009
    if "number" in df.columns:
        extracted = df["number"].astype(str).str.extract(r"([A-Za-z]+)-?(\d+)?")
        df["message_prefix"] = extracted[0].replace("nan", pd.NA)
        df["message_code"] = pd.to_numeric(extracted[1], errors="coerce")

    # Add ETL metadata
    df = add_metadata_columns(df, file_path)

    return df


# ============================================================
# MAIN
# ============================================================

def main():
    files = sorted(BRONZE_DIR.glob("*.dat"))
    print(f"Found {len(files)} Info_Message_History file(s).")

    for file_path in files:
        try:
            output_path = build_output_path(file_path)

            if output_path.exists():
                print(f"[SKIP] {file_path.name}")
                continue

            print(f"Processing {file_path.name}...")
            df_clean = clean_info_message_file(file_path)
            df_clean.to_csv(output_path, index=False)
            print(f"[OK] {output_path}")

        except Exception as e:
            print(f"[ERROR] {file_path.name} | {e}")

    print("Info_Message_History cleaning finished.")


if __name__ == "__main__":
    main()