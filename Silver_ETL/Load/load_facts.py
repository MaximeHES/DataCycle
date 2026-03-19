import pandas as pd
import numpy as np
import logging
from pathlib import Path
from sqlalchemy import text

from config import (
    PATHS, BATCH_SIZE, PRODUCT_KEY_MAP,
    HOPPER_KEY_MAP, STOP_KEY_MAP, POWDER_STATUS_MAP,
    TABS_STATUS_MAP, DETERGENT_STATUS_MAP, RINSE_TYPE_MAP,
    FLOW_STATUS_MAP, NOZZLE_STATUS_MAP, SILVER_METADATA_COLS,
)
from connection import get_sqlalchemy_engine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# MODULE-LEVEL CACHE FOR MACHINE KEYS
# ──────────────────────────────────────────────
_machine_key_cache = {}


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────

def read_silver_files(folder_path: Path) -> pd.DataFrame:
    """Read all CSV/parquet files in a silver folder and all subfolders into one DataFrame."""
    frames = []
    folder = Path(folder_path)

    if not folder.exists():
        logger.warning(f"Folder not found: {folder}")
        return pd.DataFrame()

    files = sorted([f for f in folder.rglob("*") if f.is_file()])

    if not files:
        logger.warning(f"No files found at all in {folder}")
        return pd.DataFrame()

    data_files_count = 0

    for f in files:
        try:
            if f.suffix.lower() == ".parquet":
                frames.append(pd.read_parquet(f))
                data_files_count += 1
            elif f.suffix.lower() in (".csv", ".dat", ".txt"):
                frames.append(pd.read_csv(f, sep=",", encoding="utf-8"))
                data_files_count += 1
            else:
                logger.debug(f"Skipping unknown file type: {f}")
        except Exception as e:
            logger.error(f"Failed reading file {f}: {e}")

    if not frames:
        logger.warning(f"No supported data files found in {folder}")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    logger.info(f"  Read {len(df)} rows from {data_files_count} files in {folder}")
    return df


def drop_silver_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Remove silver-layer metadata columns that don't belong in fact tables."""
    cols_to_drop = [c for c in SILVER_METADATA_COLS if c in df.columns]
    return df.drop(columns=cols_to_drop)


def compute_date_key(ts: pd.Series) -> pd.Series:
    return pd.to_datetime(ts).dt.strftime("%Y%m%d").astype(int)


def compute_time_key(ts: pd.Series) -> pd.Series:
    dt = pd.to_datetime(ts)
    return dt.dt.hour * 3600 + dt.dt.minute * 60 + dt.dt.second


def safe_map(series: pd.Series, mapping: dict, default=None):
    return series.map(mapping).fillna(default if default is not None else np.nan)


# ──────────────────────────────────────────────
# DYNAMIC MACHINE RESOLUTION
# ──────────────────────────────────────────────

def get_machine_key_map(engine, force_refresh=False):
    """
    Load machine_id → machine_key mapping from dim_machine.
    Uses a module-level cache; pass force_refresh=True after inserts.
    """
    global _machine_key_cache
    if _machine_key_cache and not force_refresh:
        return _machine_key_cache

    df = pd.read_sql("SELECT machine_key, machine_id FROM dim_machine", engine)
    _machine_key_cache = dict(zip(df["machine_id"], df["machine_key"]))
    logger.info(f"  Machine key cache loaded: {len(_machine_key_cache)} machines")
    return _machine_key_cache


def ensure_machines_exist(machine_ids, engine):
    """
    Check a list of machine_ids against dim_machine.
    Auto-insert any new ones with a placeholder name like 'Unknown-5044'.
    Returns the full (refreshed) mapping.
    """
    current_map = get_machine_key_map(engine)
    new_ids = [int(mid) for mid in machine_ids if int(mid) not in current_map]

    if not new_ids:
        return current_map

    logger.info(f"  Found {len(new_ids)} NEW machine_id(s) not in dim_machine: {new_ids}")

    with engine.begin() as conn:
        for mid in new_ids:
            conn.execute(
                text(
                    "INSERT INTO dim_machine (machine_id, machine_name) "
                    "VALUES (:mid, :mname)"
                ),
                {"mid": mid, "mname": f"Unknown-{mid}"},
            )
            logger.info(f"    Inserted machine_id={mid} as 'Unknown-{mid}'")

    # Refresh cache after inserts
    return get_machine_key_map(engine, force_refresh=True)


def resolve_machine_keys(df: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Map machine_id → machine_key for the entire DataFrame.
    Auto-inserts unknown machines into dim_machine first.
    """
    unique_ids = df["machine_id"].dropna().unique().tolist()
    machine_map = ensure_machines_exist(unique_ids, engine)
    df["machine_key"] = df["machine_id"].map(machine_map)

    still_unmapped = df["machine_key"].isna().sum()
    if still_unmapped > 0:
        logger.error(f"  {still_unmapped} rows STILL unmapped after auto-insert — this should not happen")

    return df


# ──────────────────────────────────────────────
# BULK INSERT
# ──────────────────────────────────────────────

def bulk_insert(df: pd.DataFrame, table_name: str, engine, batch_size: int = BATCH_SIZE):
    total = len(df)
    if total == 0:
        logger.info(f"  No rows to insert into {table_name}")
        return

    for start in range(0, total, batch_size):
        batch = df.iloc[start:start + batch_size]
        batch.to_sql(table_name, engine, if_exists="append", index=False, method="multi")
        logger.info(f"  {table_name}: inserted rows {start+1}–{min(start+batch_size, total)} / {total}")


# ──────────────────────────────────────────────
# FACT: PRODUCTION
# ──────────────────────────────────────────────

def load_fact_production(engine):
    logger.info("=" * 40)
    logger.info("Loading fact_production...")
    df = read_silver_files(PATHS["product"])
    if df.empty:
        return

    df = drop_silver_metadata(df)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Resolve keys
    df = resolve_machine_keys(df, engine)
    df["date_key"] = compute_date_key(df["timestamp"])
    df["time_key"] = compute_time_key(df["timestamp"])
    df["product_key"] = safe_map(df["prod_type"], PRODUCT_KEY_MAP)
    df["hopper_key"] = safe_map(df["bean_hopper"], HOPPER_KEY_MAP)
    df["stop_key"] = safe_map(df["stopped"], STOP_KEY_MAP)

    # outlet_side: 0 → LEFT, 1 → RIGHT (stored as NVARCHAR(10))
    df["outlet_side"] = df["outlet_side"].map({0: "LEFT", 1: "RIGHT"})

    # is_double from double_prod
    df["is_double"] = df["double_prod"].apply(lambda x: 1 if x == 1 else 0)

    # is_coffee_extraction and is_milk_event already in silver — use directly

    fact = pd.DataFrame({
        "machine_key":          df["machine_key"],
        "date_key":             df["date_key"],
        "time_key":             df["time_key"],
        "product_key":          df["product_key"],
        "hopper_key":           df["hopper_key"],
        "stop_key":             df["stop_key"],
        "outlet_side":          df["outlet_side"],
        "is_double":            df["is_double"],
        "press_before":         df["press_before"],
        "press_after":          df["press_after"],
        "press_final":          df["press_final"],
        "grind_time_sec":       df["grind_time"],
        "extraction_time_sec":  df["ext_time"],
        "milk_time_sec":        df["milk_time"],
        "water_qnty_ticks":     df["water_qnty"],
        "water_temp_c":         df["water_temp"],
        "milk_temp_c":          df["milk_temp"],
        "boiler_temp_c":        df["boiler_temp"],
        "steam_pressure_bar":   df["steam_pressure"],
        "grind_adjust_left":    df["grind_adjust_left"],
        "grind_adjust_right":   df["grind_adjust_right"],
        "is_coffee_extraction": df["is_coffee_extraction"],
        "is_milk_event":        df["is_milk_event"],
        "source_timestamp":     df["timestamp"],
    })

    before = len(fact)
    fact = fact.dropna(subset=["machine_key"])
    if before - len(fact) > 0:
        logger.warning(f"  Dropped {before - len(fact)} rows with unknown machine_id")

    fact["machine_key"] = fact["machine_key"].astype(int)
    fact["date_key"] = fact["date_key"].astype(int)
    fact["time_key"] = fact["time_key"].astype(int)
    for col in ["product_key", "hopper_key", "stop_key"]:
        fact[col] = fact[col].astype("Int64")

    bulk_insert(fact, "fact_production", engine)
    logger.info(f"fact_production done: {len(fact)} rows loaded.")


# ──────────────────────────────────────────────
# FACT: CLEANING
# ──────────────────────────────────────────────

def load_fact_cleaning(engine):
    logger.info("=" * 40)
    logger.info("Loading fact_cleaning...")
    df = read_silver_files(PATHS["cleaning"])
    if df.empty:
        return

    df = drop_silver_metadata(df)
    df["timestamp_start"] = pd.to_datetime(df["timestamp_start"])
    df["timestamp_end"] = pd.to_datetime(df["timestamp_end"])

    df["duration_sec"] = (df["timestamp_end"] - df["timestamp_start"]).dt.total_seconds()

    df = resolve_machine_keys(df, engine)
    df["date_key"] = compute_date_key(df["timestamp_start"])
    df["time_key"] = compute_time_key(df["timestamp_start"])
    df["powder_status_key"] = safe_map(df["powder_clean_status"], POWDER_STATUS_MAP)
    df["tabs_status_left_key"] = safe_map(df["tabs_status_left"], TABS_STATUS_MAP)
    df["tabs_status_right_key"] = safe_map(df["tabs_status_right"], TABS_STATUS_MAP)
    df["detergent_status_left_key"] = safe_map(df["detergent_status_left"], DETERGENT_STATUS_MAP)
    df["detergent_status_right_key"] = safe_map(df["detergent_status_right"], DETERGENT_STATUS_MAP)

    fact = pd.DataFrame({
        "machine_key":                  df["machine_key"],
        "date_key":                     df["date_key"],
        "time_key":                     df["time_key"],
        "powder_status_key":            df["powder_status_key"],
        "tabs_status_left_key":         df["tabs_status_left_key"],
        "tabs_status_right_key":        df["tabs_status_right_key"],
        "detergent_status_left_key":    df["detergent_status_left_key"],
        "detergent_status_right_key":   df["detergent_status_right_key"],
        "duration_sec":                 df["duration_sec"],
        "milk_pump_error_left":         df["milk_pump_error_left"],
        "milk_pump_error_right":        df["milk_pump_error_right"],
        "milk_clean_temp_left":         df["milk_clean_temp_left"],
        "milk_clean_temp_right":        df["milk_clean_temp_right"],
        "milk_clean_rpm_left":          df["milk_clean_rpm_left"],
        "milk_clean_rpm_right":         df["milk_clean_rpm_right"],
        "milk_seq_cycle_left":          df["milk_seq_cycle_left"],
        "milk_seq_cycle_right":         df["milk_seq_cycle_right"],
        "milk_seq_cycle_left_1":        df["milk_seq_cycle_left_1"],
        "milk_seq_cycle_left_2":        df["milk_seq_cycle_left_2"],
        "milk_seq_cycle_right_1":       df["milk_seq_cycle_right_1"],
        "milk_seq_cycle_right_2":       df["milk_seq_cycle_right_2"],
        "milk_temp_left_1":             df["milk_temp_left_1"],
        "milk_temp_left_2":             df["milk_temp_left_2"],
        "milk_temp_right_1":            df["milk_temp_right_1"],
        "milk_temp_right_2":            df["milk_temp_right_2"],
        "milk_rpm_left_1":              df["milk_rpm_left_1"],
        "milk_rpm_left_2":              df["milk_rpm_left_2"],
        "milk_rpm_right_1":             df["milk_rpm_right_1"],
        "milk_rpm_right_2":             df["milk_rpm_right_2"],
        "milk_clean_temp_left_part1":   df["milk_clean_temp_left_part1"],
        "milk_clean_temp_left_part2":   df["milk_clean_temp_left_part2"],
        "milk_clean_temp_right_part1":  df["milk_clean_temp_right_part1"],
        "milk_clean_temp_right_part2":  df["milk_clean_temp_right_part2"],
        "milk_clean_rpm_left_part1":    df["milk_clean_rpm_left_part1"],
        "milk_clean_rpm_left_part2":    df["milk_clean_rpm_left_part2"],
        "milk_clean_rpm_right_part1":   df["milk_clean_rpm_right_part1"],
        "milk_clean_rpm_right_part2":   df["milk_clean_rpm_right_part2"],
        "source_timestamp_start":       df["timestamp_start"],
        "source_timestamp_end":         df["timestamp_end"],
    })

    before = len(fact)
    fact = fact.dropna(subset=["machine_key"])
    if before - len(fact) > 0:
        logger.warning(f"  Dropped {before - len(fact)} rows with unknown machine_id")

    fact["machine_key"] = fact["machine_key"].astype(int)
    fact["date_key"] = fact["date_key"].astype(int)
    fact["time_key"] = fact["time_key"].astype(int)
    for col in ["powder_status_key", "tabs_status_left_key", "tabs_status_right_key",
                "detergent_status_left_key", "detergent_status_right_key"]:
        fact[col] = fact[col].astype("Int64")

    bulk_insert(fact, "fact_cleaning", engine)
    logger.info(f"fact_cleaning done: {len(fact)} rows loaded.")


# ──────────────────────────────────────────────
# FACT: RINSE
# ──────────────────────────────────────────────

def load_fact_rinse(engine):
    logger.info("=" * 40)
    logger.info("Loading fact_rinse...")
    df = read_silver_files(PATHS["rinse"])
    if df.empty:
        return

    df = drop_silver_metadata(df)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df = resolve_machine_keys(df, engine)
    df["date_key"] = compute_date_key(df["timestamp"])
    df["time_key"] = compute_time_key(df["timestamp"])
    df["rinse_type_key"] = safe_map(df["rinse_type"], RINSE_TYPE_MAP)
    df["flow_status_left_key"] = safe_map(df["status_left"], FLOW_STATUS_MAP)
    df["flow_status_right_key"] = safe_map(df["status_right"], FLOW_STATUS_MAP)
    df["nozzle_status_left_key"] = safe_map(df["nozzle_status_left"], NOZZLE_STATUS_MAP)
    df["nozzle_status_right_key"] = safe_map(df["nozzle_status_right"], NOZZLE_STATUS_MAP)

    # 65535 = NULL sentinel for flow rates
    for col in ["flow_rate_left", "flow_rate_right", "nozzle_flow_rate_left", "nozzle_flow_rate_right"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].replace(65535, np.nan)

    # side_active already exists in silver (lowercase: "left", "right", "both") — use directly

    fact = pd.DataFrame({
        "machine_key":              df["machine_key"],
        "date_key":                 df["date_key"],
        "time_key":                 df["time_key"],
        "rinse_type_key":           df["rinse_type_key"],
        "flow_status_left_key":     df["flow_status_left_key"],
        "flow_status_right_key":    df["flow_status_right_key"],
        "nozzle_status_left_key":   df["nozzle_status_left_key"],
        "nozzle_status_right_key":  df["nozzle_status_right_key"],
        "flow_rate_left":           df["flow_rate_left"],
        "flow_rate_right":          df["flow_rate_right"],
        "pump_pressure_bar":        df["pump_pressure"],
        "nozzle_flow_rate_left":    df["nozzle_flow_rate_left"],
        "nozzle_flow_rate_right":   df["nozzle_flow_rate_right"],
        "side_active":              df["side_active"],
        "source_timestamp":         df["timestamp"],
    })

    before = len(fact)
    fact = fact.dropna(subset=["machine_key"])
    if before - len(fact) > 0:
        logger.warning(f"  Dropped {before - len(fact)} rows with unknown machine_id")

    fact["machine_key"] = fact["machine_key"].astype(int)
    fact["date_key"] = fact["date_key"].astype(int)
    fact["time_key"] = fact["time_key"].astype(int)
    for col in ["rinse_type_key", "flow_status_left_key", "flow_status_right_key",
                "nozzle_status_left_key", "nozzle_status_right_key"]:
        fact[col] = fact[col].astype("Int64")

    bulk_insert(fact, "fact_rinse", engine)
    logger.info(f"fact_rinse done: {len(fact)} rows loaded.")


# ──────────────────────────────────────────────
# FACT: ALERTS
# ──────────────────────────────────────────────

def load_fact_alerts(engine):
    logger.info("=" * 40)
    logger.info("Loading fact_alerts...")
    df = read_silver_files(PATHS["alerts"])
    if df.empty:
        return

    df = drop_silver_metadata(df)
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # alert_code is the "number" column directly (e.g. "S-005", "W-066")
    df["alert_code"] = df["number"]

    # Look up alert_type_key from dim_alert_type
    try:
        alert_lookup = pd.read_sql("SELECT alert_type_key, alert_code FROM dim_alert_type", engine)
        alert_map = dict(zip(alert_lookup["alert_code"], alert_lookup["alert_type_key"]))
    except Exception as e:
        logger.error(f"  Could not load dim_alert_type: {e}")
        alert_map = {}

    df = resolve_machine_keys(df, engine)
    df["date_key"] = compute_date_key(df["timestamp"])
    df["time_key"] = compute_time_key(df["timestamp"])
    df["alert_type_key"] = safe_map(df["alert_code"], alert_map)

    fact = pd.DataFrame({
        "machine_key":      df["machine_key"],
        "date_key":         df["date_key"],
        "time_key":         df["time_key"],
        "alert_type_key":   df["alert_type_key"],
        "alert_code":       df["alert_code"],
        "source_timestamp": df["timestamp"],
    })

    unmapped = fact[fact["alert_type_key"].isna()]
    if len(unmapped) > 0:
        unknown_codes = df.loc[fact["alert_type_key"].isna(), "alert_code"].unique()
        logger.warning(f"  {len(unmapped)} rows have unmapped alert codes: {unknown_codes}")

    before = len(fact)
    fact = fact.dropna(subset=["machine_key"])
    if before - len(fact) > 0:
        logger.warning(f"  Dropped {before - len(fact)} rows with unknown machine_id")

    fact["machine_key"] = fact["machine_key"].astype(int)
    fact["date_key"] = fact["date_key"].astype(int)
    fact["time_key"] = fact["time_key"].astype(int)
    fact["alert_type_key"] = fact["alert_type_key"].astype("Int64")

    bulk_insert(fact, "fact_alerts", engine)
    logger.info(f"fact_alerts done: {len(fact)} rows loaded.")