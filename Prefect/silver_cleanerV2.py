"""
silver_cleaner.py  — Eversys Silver Transformation (batch-driven)
==================================================================
Reads unprocessed bronze batch JSON files produced by eversys_ingestion_V8.ps1
and cleans only the files listed in them.  Never scans the bronze directory.

Run modes
---------
  python silver_cleaner.py            # process all pending batches
  python silver_cleaner.py --dry-run  # show what would be processed, no writes

Directory layout expected
-------------------------
  Bronze  : C:\\RawData\\Eversys\\<Category>\\*.dat
  Silver  : C:\\RawData\\Eversys_Cleaned\\<Category>\\YYYY\\MM\\DD\\*_CLEANED.csv
  Batches : C:\\RawData\\_state\\Eversys_Ingestion\\batches\\batch_*.json
              → marked done by renaming to batch_*.json.done after processing

Performance expectation
-----------------------
  Incremental run (5-min cadence, ~5 new files per category):
    - 0 directory scans
    - processes exactly the files in the batch
    - well under 10 s total

Changes vs previous version
----------------------------
  - Compatible with V8 bronze ingestion (watermark-based, no manifests)
  - batch_*.json written by V8 uses bronze_path (unchanged, still works)
  - batch_*.json written by V8 may contain file_count = 0 on idle runs;
    those batches are detected and marked done immediately
  - Cleaning_History: removed split_semicolon_pair logic — the composite
    columns (milk_clean_temp_left etc.) are already pre-split by Eversys
    into explicit _1 / _2 columns; the composite columns are now dropped
"""

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

BATCH_DIR   = Path(r"C:\RawData\_state\Eversys_Ingestion\batches")
SILVER_ROOT = Path(r"C:\RawData\Eversys_Cleaned")

# Maps category name -> cleaner function
CATEGORY_CLEANERS: dict = {}   # populated by @register_cleaner below


# ==============================================================
# CLEANER REGISTRY
# ==============================================================

def register_cleaner(category: str):
    """Decorator: register a function as the cleaner for a category."""
    def decorator(fn):
        CATEGORY_CLEANERS[category] = fn
        return fn
    return decorator


# ==============================================================
# SHARED HELPERS
# ==============================================================

_TS_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})[_ ](\d{2})_(\d{2})_(\d{2})-")

def extract_file_timestamp(filename: str) -> datetime | None:
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


def normalize_datetime_series(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    dt_iso = pd.to_datetime(s, errors="coerce", format="%Y-%m-%d %H:%M:%S")
    dt_eu  = pd.to_datetime(s, errors="coerce", format="%d/%m/%Y %H:%M:%S")
    result = dt_iso.copy()
    result[result.isna()] = dt_eu[result.isna()]
    return result.dt.strftime("%Y-%m-%d %H:%M:%S")


def clean_common_strings(df: pd.DataFrame) -> pd.DataFrame:
    null_map = {"": pd.NA, "nan": pd.NA, "None": pd.NA, "NULL": pd.NA, "null": pd.NA}
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip().replace(null_map)
    return df


def read_bronze_csv(file_path: Path) -> pd.DataFrame:
    """Read a semicolon-delimited .dat file from bronze."""
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
    df["source_file"]         = source_file.name
    df["file_timestamp"]      = file_ts.strftime("%Y-%m-%d %H:%M:%S") if file_ts else pd.NA
    df["ingestion_timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df


def build_silver_path(source_file: Path, category: str) -> Path:
    file_ts = extract_file_timestamp(source_file.name)
    if not file_ts:
        raise ValueError(f"Cannot parse timestamp from: {source_file.name}")
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


# ==============================================================
# CATEGORY CLEANERS
# ==============================================================

@register_cleaner("Product_History")
def clean_product(file_path: Path) -> pd.DataFrame:
    df = read_bronze_csv(file_path)
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
    df = clean_common_strings(df)
    require_columns(df, ["machine_id", "timestamp", "prod_type"], file_path)

    df["timestamp"] = normalize_datetime_series(df["timestamp"])

    numeric_cols = [
        "machine_id", "press_before", "press_after", "press_final",
        "grind_time", "ext_time", "water_qnty", "water_temp", "prod_type",
        "double_prod", "bean_hopper", "outlet_side", "stopped",
        "milk_temp", "steam_pressure", "grind_adjust_left", "grind_adjust_right",
        "milk_time", "boiler_temp",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates()

    if {"water_qnty", "press_before", "press_after"}.issubset(df.columns):
        df["is_coffee_extraction"] = np.where(
            (df["water_qnty"].fillna(0) > 0)
            & ((df["press_before"].fillna(0) > 0) | (df["press_after"].fillna(0) > 0)),
            1, 0,
        )
    if {"milk_temp", "steam_pressure"}.issubset(df.columns):
        df["is_milk_event"] = np.where(
            (df["milk_temp"].fillna(0) > 0) | (df["steam_pressure"].fillna(0) > 0),
            1, 0,
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

    df["timestamp_end"] = normalize_datetime_series(df["timestamp_end"])

    # Drop composite pair columns — their values are already split into
    # explicit _1 / _2 columns by Eversys (e.g. milk_temp_left_1, milk_temp_left_2)
    composite_cols = [
        "milk_clean_temp_left", "milk_clean_temp_right",
        "milk_clean_rpm_left",  "milk_clean_rpm_right",
        "milk_seq_cycle_left",  "milk_seq_cycle_right",
    ]
    df = df.drop(columns=[c for c in composite_cols if c in df.columns])

    numeric_cols = [
        "machine_id", "cleaning_id", "powder_qty", "milk_clean_temp",
        "milk_clean_time", "detergent_qty", "water_qty", "error_code",
        "cleaning_status", "cleaning_type", "milk_system",
        "milk_seq_cycle_left_1",  "milk_seq_cycle_left_2",
        "milk_seq_cycle_right_1", "milk_seq_cycle_right_2",
        "milk_temp_left_1",  "milk_temp_left_2",
        "milk_temp_right_1", "milk_temp_right_2",
        "milk_rpm_left_1",   "milk_rpm_left_2",
        "milk_rpm_right_1",  "milk_rpm_right_2",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.drop_duplicates()
    return add_metadata(df, file_path)


@register_cleaner("Rinse_History")
def clean_rinse(file_path: Path) -> pd.DataFrame:
    df = read_bronze_csv(file_path)
    df.columns = df.columns.str.replace("\ufeff", "", regex=False).str.strip()
    df = clean_common_strings(df)
    require_columns(df, ["machine_id", "timestamp", "rinse_type"], file_path)

    df["timestamp"] = normalize_datetime_series(df["timestamp"])

    numeric_cols = [
        "machine_id", "rinse_type",
        "flow_rate_left", "flow_rate_right", "status_left", "status_right",
        "pump_pressure", "nozzle_flow_rate_left", "nozzle_flow_rate_right",
        "nozzle_status_left", "nozzle_status_right",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.replace(65535, pd.NA)
    df = df.drop_duplicates()

    left_active  = df["flow_rate_left"].fillna(0)  > 0 if "flow_rate_left"  in df.columns else pd.Series(False, index=df.index)
    right_active = df["flow_rate_right"].fillna(0) > 0 if "flow_rate_right" in df.columns else pd.Series(False, index=df.index)

    df["side_active"] = np.select(
        [left_active & right_active, left_active & ~right_active, ~left_active & right_active],
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

    df["timestamp"] = normalize_datetime_series(df["timestamp"])

    if "machine_id"  in df.columns: df["machine_id"]  = pd.to_numeric(df["machine_id"],  errors="coerce")
    if "type_number" in df.columns: df["type_number"] = pd.to_numeric(df["type_number"], errors="coerce")

    df = df.drop_duplicates()

    if "number" in df.columns:
        extracted = df["number"].astype(str).str.extract(r"([A-Za-z]+)-?(\d+)?")
        df["message_prefix"] = extracted[0].replace("nan", pd.NA)
        df["message_code"]   = pd.to_numeric(extracted[1], errors="coerce")

    return add_metadata(df, file_path)


# ==============================================================
# BATCH PROCESSING
# ==============================================================

def find_pending_batches() -> list[Path]:
    """Return all batch_*.json without a matching .json.done sibling, sorted oldest first."""
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
    """
    Process one batch file.  Returns a summary dict.
    Files already present in silver are skipped (idempotent).
    Batches with 0 files are marked done immediately.
    """
    print(f"\n{'[DRY-RUN] ' if dry_run else ''}Processing batch: {batch_path.name}")

    entries = load_batch(batch_path)
    summary = {
        "batch": batch_path.name,
        "total": len(entries),
        "cleaned": 0, "skipped": 0, "errors": 0, "unknown_category": 0,
    }

    # Empty batch (bronze ran but nothing was new) — mark done and move on
    if not entries:
        print("  Empty batch — nothing to clean.")
        if not dry_run:
            mark_batch_done(batch_path)
            print("  Batch marked done.")
        return summary

    # Group by category
    by_cat: dict[str, list[dict]] = {}
    for entry in entries:
        cat = entry.get("category", "")
        by_cat.setdefault(cat, []).append(entry)

    for cat, cat_entries in by_cat.items():
        cleaner = CATEGORY_CLEANERS.get(cat)
        if cleaner is None:
            print(f"  [WARN] No cleaner registered for '{cat}' — skipping {len(cat_entries)} file(s).")
            summary["unknown_category"] += len(cat_entries)
            continue

        cat_cleaned = cat_skipped = cat_errors = 0

        for entry in cat_entries:
            bronze_path = Path(entry["bronze_path"])

            try:
                silver_path = build_silver_path(bronze_path, cat)

                if silver_path.exists():
                    cat_skipped += 1
                    continue

                if dry_run:
                    print(f"  [DRY-RUN] Would clean: {bronze_path.name} -> {silver_path}")
                    cat_cleaned += 1
                    continue

                if not bronze_path.exists():
                    raise FileNotFoundError(f"Bronze file missing: {bronze_path}")

                df = cleaner(bronze_path)
                df.to_csv(silver_path, index=False)
                cat_cleaned += 1

            except Exception as e:
                cat_errors += 1
                print(f"  [ERROR] {bronze_path.name} | {e}")

        print(f"  {cat}: cleaned={cat_cleaned}  skipped={cat_skipped}  errors={cat_errors}")
        summary["cleaned"] += cat_cleaned
        summary["skipped"] += cat_skipped
        summary["errors"]  += cat_errors

    if not dry_run:
        if summary["errors"] == 0:
            mark_batch_done(batch_path)
            print("  Batch marked done.")
        else:
            print(f"  Batch NOT marked done ({summary['errors']} error(s) — will retry next run).")

    return summary


# ==============================================================
# MAIN
# ==============================================================

def main():
    parser = argparse.ArgumentParser(description="Eversys silver cleaner (batch-driven, V8 compatible)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed without writing")
    args = parser.parse_args()

    start = datetime.now()
    print(f"{'='*54}")
    print(f"Eversys Silver Cleaner  —  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Batch dir : {BATCH_DIR}")
    print(f"Silver dir: {SILVER_ROOT}")
    print(f"{'='*54}")

    if not BATCH_DIR.exists():
        print(f"Batch directory not found: {BATCH_DIR}")
        print("Nothing to do.")
        return

    pending = find_pending_batches()
    print(f"Pending batches: {len(pending)}")

    if not pending:
        print("Nothing to do.")
        return

    totals = {"total": 0, "cleaned": 0, "skipped": 0, "errors": 0}

    for batch_path in pending:
        summary = process_batch(batch_path, dry_run=args.dry_run)
        for k in totals:
            totals[k] += summary.get(k, 0)

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{'='*54}")
    print(f"Done in {elapsed:.1f}s")
    print(f"Total files : {totals['total']}")
    print(f"Cleaned     : {totals['cleaned']}")
    print(f"Skipped     : {totals['skipped']}")
    print(f"Errors      : {totals['errors']}")
    print(f"{'='*54}")

    if totals["errors"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
