import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


# ==============================================================
# CONFIG
# ==============================================================

BATCH_DIR = Path(r"C:\RawData\_state\Eversys_Ingestion\batches")
SILVER_ROOT = Path(r"C:\RawData\Eversys_Cleaned")

CATEGORY_CLEANERS: dict = {}


# ==============================================================
# CLEANER REGISTRY
# ==============================================================

def register_cleaner(category: str):
    def decorator(fn):
        CATEGORY_CLEANERS[category] = fn
        return fn
    return decorator


# ==============================================================
# HELPERS
# ==============================================================

_TS_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})[_ ](\d{2})_(\d{2})_(\d{2})-")


def extract_file_timestamp(filename: str):
    m = _TS_PATTERN.match(filename)
    if not m:
        return None

    try:
        return datetime.strptime(
            f"{m.group(1)} {m.group(2)}:{m.group(3)}:{m.group(4)}",
            "%Y-%m-%d %H:%M:%S",
        )
    except ValueError:
        return None


def normalize_datetime_series(series: pd.Series, col_name: str = "unknown") -> pd.Series:
    """
    Supports:
      - ISO: YYYY-MM-DD HH:MM:SS
      - EU : DD/MM/YYYY HH:MM:SS
      - US : MM/DD/YYYY HH:MM:SS
    Returns pandas datetime dtype.
    """
    s = series.copy()

    # Preserve real empties as NA before string ops
    s = s.replace("", pd.NA)
    s = s.astype("string").str.strip()

    dt_iso = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d %H:%M:%S")
    dt_eu = pd.to_datetime(s, errors="coerce", format="%d/%m/%Y %H:%M:%S")
    dt_us = pd.to_datetime(s, errors="coerce", format="%m/%d/%Y %H:%M:%S")

    result = dt_iso.copy()
    result[result.isna()] = dt_eu[result.isna()]
    result[result.isna()] = dt_us[result.isna()]

    original_non_null = s.notna()
    invalid_mask = result.isna() & original_non_null
    if invalid_mask.any():
        print(f"[WARNING] {invalid_mask.sum()} invalid datetime in {col_name}")
        print(s[invalid_mask].head(5))

    return result


def clean_common_strings(df: pd.DataFrame) -> pd.DataFrame:
    null_map = {
        "": pd.NA,
        "nan": pd.NA,
        "None": pd.NA,
        "NULL": pd.NA,
        "null": pd.NA,
    }

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].str.strip()
            df[col] = df[col].replace(null_map)

    return df


def read_bronze_csv(file_path: Path) -> pd.DataFrame:
    return pd.read_csv(
        file_path,
        sep=";",
        encoding="utf-8-sig",
        dtype=str,
        keep_default_na=False,
        engine="python",
    )


def add_metadata(df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
    file_ts = extract_file_timestamp(source_file.name)
    df["source_file"] = source_file.name
    df["file_timestamp"] = file_ts.strftime("%Y-%m-%d %H:%M:%S") if file_ts else pd.NA
    df["ingestion_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df


def build_silver_path(source_file: Path, category: str) -> Path:
    file_ts = extract_file_timestamp(source_file.name)
    if not file_ts:
        raise ValueError(f"Cannot parse timestamp from filename: {source_file.name}")

    target_dir = (
        SILVER_ROOT / category
        / file_ts.strftime("%Y")
        / file_ts.strftime("%m")
        / file_ts.strftime("%d")
    )
    target_dir.mkdir(parents=True, exist_ok=True)

    return target_dir / f"{source_file.stem}_CLEANED.csv"


def require_columns(df: pd.DataFrame, cols: list[str], file_path: Path) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns {missing} in {file_path.name}")


def convert_numeric_columns(df: pd.DataFrame, numeric_cols: list[str]) -> pd.DataFrame:
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


# ==============================================================
# CATEGORY CLEANERS
# ==============================================================

@register_cleaner("Product_History")
def clean_product(file_path: Path) -> pd.DataFrame:
    df = read_bronze_csv(file_path)
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
    df = clean_common_strings(df)

    require_columns(df, ["machine_id", "timestamp", "prod_type"], file_path)

    df["timestamp"] = normalize_datetime_series(df["timestamp"], "timestamp")

    numeric_cols = [
        "machine_id", "press_before", "press_after", "press_final",
        "grind_time", "ext_time", "water_qnty", "water_temp", "prod_type",
        "double_prod", "bean_hopper", "outlet_side", "stopped",
        "milk_temp", "steam_pressure", "grind_adjust_left", "grind_adjust_right",
        "milk_time", "boiler_temp",
    ]
    df = convert_numeric_columns(df, numeric_cols)

    df = df.drop_duplicates()

    if {"water_qnty", "press_before", "press_after"}.issubset(df.columns):
        df["is_coffee_extraction"] = np.where(
            (df["water_qnty"].fillna(0) > 0)
            & (
                (df["press_before"].fillna(0) > 0)
                | (df["press_after"].fillna(0) > 0)
            ),
            1,
            0,
        )

    if {"milk_temp", "steam_pressure"}.issubset(df.columns):
        df["is_milk_event"] = np.where(
            (df["milk_temp"].fillna(0) > 0)
            | (df["steam_pressure"].fillna(0) > 0),
            1,
            0,
        )

    return add_metadata(df, file_path)


@register_cleaner("Cleaning_History")
def clean_cleaning(file_path: Path) -> pd.DataFrame:
    df = read_bronze_csv(file_path)
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()

    for col in df.columns:
        df[col] = df[col].astype(str).str.strip().str.strip('"')

    df = clean_common_strings(df)

    require_columns(df, ["machine_id", "timestamp_end"], file_path)

    if "timestamp_start" in df.columns:
        df["timestamp_start"] = normalize_datetime_series(df["timestamp_start"], "timestamp_start")

    df["timestamp_end"] = normalize_datetime_series(df["timestamp_end"], "timestamp_end")

    # Drop composite pair columns because values are already split by Eversys
    # into explicit _1 / _2 columns.
    composite_cols = [
        "milk_clean_temp_left", "milk_clean_temp_right",
        "milk_clean_rpm_left", "milk_clean_rpm_right",
        "milk_seq_cycle_left", "milk_seq_cycle_right",
    ]
    df = df.drop(columns=[c for c in composite_cols if c in df.columns])

    numeric_cols = [
        "machine_id", "cleaning_id", "powder_qty", "milk_clean_temp",
        "milk_clean_time", "detergent_qty", "water_qty", "error_code",
        "cleaning_status", "cleaning_type", "milk_system",
        "milk_seq_cycle_left_1", "milk_seq_cycle_left_2",
        "milk_seq_cycle_right_1", "milk_seq_cycle_right_2",
        "milk_temp_left_1", "milk_temp_left_2",
        "milk_temp_right_1", "milk_temp_right_2",
        "milk_rpm_left_1", "milk_rpm_left_2",
        "milk_rpm_right_1", "milk_rpm_right_2",
    ]
    df = convert_numeric_columns(df, numeric_cols)

    df = df.drop_duplicates()

    return add_metadata(df, file_path)


@register_cleaner("Rinse_History")
def clean_rinse(file_path: Path) -> pd.DataFrame:
    df = read_bronze_csv(file_path)
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
    df = clean_common_strings(df)

    require_columns(df, ["machine_id", "timestamp", "rinse_type"], file_path)

    df["timestamp"] = normalize_datetime_series(df["timestamp"], "timestamp")

    numeric_cols = [
        "machine_id", "rinse_type",
        "flow_rate_left", "flow_rate_right",
        "status_left", "status_right",
        "pump_pressure", "nozzle_flow_rate_left", "nozzle_flow_rate_right",
        "nozzle_status_left", "nozzle_status_right",
    ]
    df = convert_numeric_columns(df, numeric_cols)

    # Sentinel handling scoped to rinse-specific columns
    sentinel_cols = [
        "flow_rate_left", "flow_rate_right",
        "nozzle_flow_rate_left", "nozzle_flow_rate_right",
    ]
    for col in sentinel_cols:
        if col in df.columns:
            df[col] = df[col].replace(65535, pd.NA)

    df = df.drop_duplicates()

    left_active = (
        df["flow_rate_left"].fillna(0) > 0
        if "flow_rate_left" in df.columns
        else pd.Series(False, index=df.index)
    )
    right_active = (
        df["flow_rate_right"].fillna(0) > 0
        if "flow_rate_right" in df.columns
        else pd.Series(False, index=df.index)
    )

    df["side_active"] = np.select(
        [
            left_active & right_active,
            left_active & ~right_active,
            ~left_active & right_active,
        ],
        ["both", "left", "right"],
        default="none",
    )

    return add_metadata(df, file_path)


@register_cleaner("Info_Message_History")
def clean_info_message(file_path: Path) -> pd.DataFrame:
    df = read_bronze_csv(file_path)
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
    df = clean_common_strings(df)

    require_columns(df, ["machine_id", "timestamp", "number"], file_path)

    df["timestamp"] = normalize_datetime_series(df["timestamp"], "timestamp")

    if "machine_id" in df.columns:
        df["machine_id"] = pd.to_numeric(df["machine_id"], errors="coerce")

    if "type_number" in df.columns:
        df["type_number"] = pd.to_numeric(df["type_number"], errors="coerce")

    df = df.drop_duplicates()

    if "number" in df.columns:
        extracted = df["number"].astype("string").str.extract(r"([A-Za-z]+)-?(\d+)?")
        df["message_prefix"] = extracted[0].replace("nan", pd.NA)
        df["message_code"] = pd.to_numeric(extracted[1], errors="coerce")

    return add_metadata(df, file_path)


# ==============================================================
# BATCH PROCESSING
# ==============================================================

def find_pending_batches() -> list[Path]:
    if not BATCH_DIR.exists():
        return []

    all_batches = sorted(BATCH_DIR.glob("batch_*.json"))
    return [b for b in all_batches if not b.with_suffix(".json.done").exists()]


def load_batch(batch_path: Path) -> list[dict]:
    with open(batch_path, encoding="utf-8-sig") as f:
        payload = json.load(f)
    return payload.get("files", [])


def mark_batch_done(batch_path: Path) -> None:
    batch_path.rename(batch_path.with_suffix(".json.done"))


def process_batch(batch_path: Path, dry_run: bool = False) -> dict:
    print(f"\nProcessing batch: {batch_path.name}")

    entries = load_batch(batch_path)
    summary = {
        "batch": batch_path.name,
        "total": len(entries),
        "cleaned": 0,
        "skipped": 0,
        "errors": 0,
        "unknown_category": 0,
    }

    if not entries:
        print("  Empty batch")
        if not dry_run:
            mark_batch_done(batch_path)
        return summary

    print(f"  Files: {len(entries)}")

    by_cat: dict[str, list[dict]] = {}
    for entry in entries:
        cat = entry.get("category", "")
        by_cat.setdefault(cat, []).append(entry)

    for cat, cat_entries in by_cat.items():
        cleaner = CATEGORY_CLEANERS.get(cat)

        if cleaner is None:
            print(f"  [WARN] No cleaner for {cat}")
            summary["unknown_category"] += len(cat_entries)
            continue

        cat_cleaned = 0
        cat_skipped = 0
        cat_errors = 0

        for entry in cat_entries:
            bronze_path = Path(entry["bronze_path"])
            print(f"  → {bronze_path.name}")

            try:
                silver_path = build_silver_path(bronze_path, cat)

                if silver_path.exists():
                    print("    skipped (already exists)")
                    cat_skipped += 1
                    continue

                if dry_run:
                    print(f"    [DRY-RUN] would clean -> {silver_path}")
                    cat_cleaned += 1
                    continue

                if not bronze_path.exists():
                    raise FileNotFoundError(f"Bronze file missing: {bronze_path}")

                df = cleaner(bronze_path)
                df.to_csv(silver_path, index=False)

                cat_cleaned += 1

            except Exception as e:
                cat_errors += 1
                print(f"    [ERROR] {e}")

        print(f"  {cat}: cleaned={cat_cleaned} skipped={cat_skipped} errors={cat_errors}")

        summary["cleaned"] += cat_cleaned
        summary["skipped"] += cat_skipped
        summary["errors"] += cat_errors

    if not dry_run:
        if summary["errors"] == 0:
            mark_batch_done(batch_path)
        else:
            print(f"  Batch NOT marked done ({summary['errors']} error(s))")

    return summary


# ==============================================================
# MAIN
# ==============================================================

def main():
    parser = argparse.ArgumentParser(description="Eversys Silver Cleaner V3")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without writing")
    args = parser.parse_args()

    print("================================================")
    print("SILVER CLEANER V3 START")
    print("================================================")
    print(f"Batch dir : {BATCH_DIR}")
    print(f"Silver dir: {SILVER_ROOT}")

    pending = find_pending_batches()

    print(f"Pending batches: {len(pending)}")

    if not pending:
        print("Nothing to do.")
        return

    totals = {
        "total": 0,
        "cleaned": 0,
        "skipped": 0,
        "errors": 0,
    }

    for batch in pending:
        summary = process_batch(batch, dry_run=args.dry_run)
        totals["total"] += summary["total"]
        totals["cleaned"] += summary["cleaned"]
        totals["skipped"] += summary["skipped"]
        totals["errors"] += summary["errors"]

    print("================================================")
    print("DONE")
    print(f"Total files : {totals['total']}")
    print(f"Cleaned     : {totals['cleaned']}")
    print(f"Skipped     : {totals['skipped']}")
    print(f"Errors      : {totals['errors']}")
    print("================================================")

    if totals["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()