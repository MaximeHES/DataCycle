from pathlib import Path
import hashlib
import json

import pandas as pd

from gold_utils_v3 import (
    DEFAULT_GOLD_STATE_DIR,
    DEFAULT_SILVER_ROOT,
    get_connection,
    setup_logger,
    load_state,
    save_state,
    list_new_silver_files,
    get_date_key,
    get_time_key,
    get_or_create_machine,
    get_powder_status_key,
    get_tabs_status_key,
    get_detergent_status_key,
    _f,
    _i,
    _dt,
)

CATEGORY = "Cleaning_History"
STATE_FILE = DEFAULT_GOLD_STATE_DIR / "cleaning_state.json"
BATCH_SIZE = 500
log = setup_logger(__name__, "gold_cleaning")

TEMP_TABLE_SQL = """
IF OBJECT_ID('tempdb..#stg_fact_cleaning') IS NOT NULL
    DROP TABLE #stg_fact_cleaning;

CREATE TABLE #stg_fact_cleaning (
    machine_key INT NOT NULL,
    date_key INT NOT NULL,
    time_key INT NOT NULL,
    powder_status_key INT NULL,
    tabs_status_left_key INT NULL,
    tabs_status_right_key INT NULL,
    detergent_status_left_key INT NULL,
    detergent_status_right_key INT NULL,
    source_timestamp_start DATETIME2(0) NULL,
    source_timestamp_end DATETIME2(0) NULL,
    duration_sec INT NULL,
    milk_pump_error_left FLOAT NULL,
    milk_pump_error_right FLOAT NULL,
    milk_seq_cycle_left_1 FLOAT NULL,
    milk_seq_cycle_left_2 FLOAT NULL,
    milk_seq_cycle_right_1 FLOAT NULL,
    milk_seq_cycle_right_2 FLOAT NULL,
    milk_temp_left_1 FLOAT NULL,
    milk_temp_left_2 FLOAT NULL,
    milk_temp_right_1 FLOAT NULL,
    milk_temp_right_2 FLOAT NULL,
    milk_rpm_left_1 FLOAT NULL,
    milk_rpm_left_2 FLOAT NULL,
    milk_rpm_right_1 FLOAT NULL,
    milk_rpm_right_2 FLOAT NULL,
    machine_type NVARCHAR(20) NULL,
    milk_machine_type NVARCHAR(20) NULL,
    source_file_path NVARCHAR(500) NULL,
    source_row_number INT NOT NULL,
    source_row_hash CHAR(64) NOT NULL
);
"""

DROP_TEMP_SQL = """
IF OBJECT_ID('tempdb..#stg_fact_cleaning') IS NOT NULL
    DROP TABLE #stg_fact_cleaning;
"""

INSERT_STAGE_SQL = """
INSERT INTO #stg_fact_cleaning (
    machine_key,
    date_key,
    time_key,
    powder_status_key,
    tabs_status_left_key,
    tabs_status_right_key,
    detergent_status_left_key,
    detergent_status_right_key,
    source_timestamp_start,
    source_timestamp_end,
    duration_sec,
    milk_pump_error_left,
    milk_pump_error_right,
    milk_seq_cycle_left_1,
    milk_seq_cycle_left_2,
    milk_seq_cycle_right_1,
    milk_seq_cycle_right_2,
    milk_temp_left_1,
    milk_temp_left_2,
    milk_temp_right_1,
    milk_temp_right_2,
    milk_rpm_left_1,
    milk_rpm_left_2,
    milk_rpm_right_1,
    milk_rpm_right_2,
    machine_type,
    milk_machine_type,
    source_file_path,
    source_row_number,
    source_row_hash
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

INSERT_FINAL_SQL = """
INSERT INTO gold.fact_cleaning (
    machine_key,
    date_key,
    time_key,
    powder_status_key,
    tabs_status_left_key,
    tabs_status_right_key,
    detergent_status_left_key,
    detergent_status_right_key,
    source_timestamp_start,
    source_timestamp_end,
    duration_sec,
    milk_pump_error_left,
    milk_pump_error_right,
    milk_seq_cycle_left_1,
    milk_seq_cycle_left_2,
    milk_seq_cycle_right_1,
    milk_seq_cycle_right_2,
    milk_temp_left_1,
    milk_temp_left_2,
    milk_temp_right_1,
    milk_temp_right_2,
    milk_rpm_left_1,
    milk_rpm_left_2,
    milk_rpm_right_1,
    milk_rpm_right_2,
    machine_type,
    milk_machine_type,
    source_file_path,
    source_row_number,
    source_row_hash
)
SELECT
    s.machine_key,
    s.date_key,
    s.time_key,
    s.powder_status_key,
    s.tabs_status_left_key,
    s.tabs_status_right_key,
    s.detergent_status_left_key,
    s.detergent_status_right_key,
    s.source_timestamp_start,
    s.source_timestamp_end,
    s.duration_sec,
    s.milk_pump_error_left,
    s.milk_pump_error_right,
    s.milk_seq_cycle_left_1,
    s.milk_seq_cycle_left_2,
    s.milk_seq_cycle_right_1,
    s.milk_seq_cycle_right_2,
    s.milk_temp_left_1,
    s.milk_temp_left_2,
    s.milk_temp_right_1,
    s.milk_temp_right_2,
    s.milk_rpm_left_1,
    s.milk_rpm_left_2,
    s.milk_rpm_right_1,
    s.milk_rpm_right_2,
    s.machine_type,
    s.milk_machine_type,
    s.source_file_path,
    s.source_row_number,
    s.source_row_hash
FROM #stg_fact_cleaning s
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.fact_cleaning f
    WHERE f.source_row_hash = s.source_row_hash
);
"""


def build_row_hash(record: dict) -> str:
    payload = json.dumps(record, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

# New specific function for the duration calcul issue
def _dt_cleaning(val):

    if val is None:
        return None

    s = str(val).strip()
    if s.lower() in ("", "nan", "none", "null", "<na>"):
        return None

    try:
        # FORCE format: DD/MM/YYYY HH:MM:SS
        ts = pd.to_datetime(s, format="%d/%m/%Y %H:%M:%S", errors="coerce")
        return None if pd.isna(ts) else ts.to_pydatetime()
    except Exception:
        return None


def _normalize_for_hash(value):
    if value is None:
        return None
    if isinstance(value, float):
        return round(value, 8)
    return value


def _derive_machine_type(row: pd.Series) -> str | None:
    det_r = _i(row.get("detergent_status_right"))
    tabs_r = _i(row.get("tabs_status_right"))
    if det_r == 4 or tabs_r == 4:
        return "Simple"
    return "Double"


def _derive_milk_machine_type(row: pd.Series) -> str:
    milk_cols = [
        "milk_seq_cycle_left_1", "milk_seq_cycle_left_2",
        "milk_seq_cycle_right_1", "milk_seq_cycle_right_2",
        "milk_temp_left_1", "milk_temp_left_2",
        "milk_temp_right_1", "milk_temp_right_2",
        "milk_rpm_left_1", "milk_rpm_left_2",
        "milk_rpm_right_1", "milk_rpm_right_2",
    ]
    for c in milk_cols:
        v = _f(row.get(c))
        if v is not None and v > 0:
            return "MilkMachine"
    return "WithoutMilkMachine"

from datetime import datetime


def _parse_dt_exact(val, fmt: str):
    if val is None:
        return None

    s = str(val).strip()
    if s.lower() in ("", "nan", "none", "null", "<na>"):
        return None

    try:
        return datetime.strptime(s, fmt)
    except Exception:
        return None


def _parse_cleaning_timestamp_pair(start_val, end_val):
    """
    Cleaning-specific timestamp parser.

    Expected normal format:
      YYYY-MM-DD HH:MM:SS

    But some rows appear to have end timestamp written as:
      YYYY-DD-MM HH:MM:SS

    Strategy:
    - parse both with normal ISO first
    - if end < start, try alternate format for end
    - accept alternate end only if it becomes >= start and duration is plausible
    """
    start_dt = _parse_dt_exact(start_val, "%Y-%m-%d %H:%M:%S")
    end_dt = _parse_dt_exact(end_val, "%Y-%m-%d %H:%M:%S")

    if start_dt is None and end_dt is None:
        return None, None

    # If already consistent, keep as-is
    if start_dt is not None and end_dt is not None and end_dt >= start_dt:
        return start_dt, end_dt

    # Try alternate interpretation for end only: YYYY-DD-MM
    alt_end_dt = _parse_dt_exact(end_val, "%Y-%d-%m %H:%M:%S")

    if start_dt is not None and alt_end_dt is not None:
        duration_sec = (alt_end_dt - start_dt).total_seconds()

        # Keep only if it makes business sense for a cleaning cycle
        # Here: 0 sec to 4 hours max
        if 0 <= duration_sec <= 4 * 3600:
            return start_dt, alt_end_dt

    # Optional: also try alternate interpretation for start if needed
    alt_start_dt = _parse_dt_exact(start_val, "%Y-%d-%m %H:%M:%S")

    if alt_start_dt is not None and end_dt is not None:
        duration_sec = (end_dt - alt_start_dt).total_seconds()
        if 0 <= duration_sec <= 4 * 3600:
            return alt_start_dt, end_dt

    # Fallback: return what we got
    return start_dt, end_dt


def _row_to_stage_tuple(conn, row: pd.Series, source_file_path: str, source_row_number: int):
    ts_start, ts_end = _parse_cleaning_timestamp_pair(
        row.get("timestamp_start"),
        row.get("timestamp_end"),
    )
    anchor = ts_end or ts_start
    machine_id = _i(row.get("machine_id"))

    if anchor is None or machine_id is None:
        return None

    machine_key = get_or_create_machine(conn, machine_id)
    date_key = get_date_key(conn, anchor)
    time_key = get_time_key(anchor)

    powder_status_key = get_powder_status_key(conn, row.get("powder_clean_status"))
    tabs_status_left_key = get_tabs_status_key(conn, row.get("tabs_status_left"))
    tabs_status_right_key = get_tabs_status_key(conn, row.get("tabs_status_right"))
    detergent_status_left_key = get_detergent_status_key(conn, row.get("detergent_status_left"))
    detergent_status_right_key = get_detergent_status_key(conn, row.get("detergent_status_right"))

    duration_sec = None
    if ts_start and ts_end:
        raw_duration = int((ts_end - ts_start).total_seconds())
        if 0 <= raw_duration <= 4 * 3600:
            duration_sec = raw_duration

    milk_pump_error_left = _f(row.get("milk_pump_error_left"))
    milk_pump_error_right = _f(row.get("milk_pump_error_right"))
    milk_seq_cycle_left_1 = _f(row.get("milk_seq_cycle_left_1"))
    milk_seq_cycle_left_2 = _f(row.get("milk_seq_cycle_left_2"))
    milk_seq_cycle_right_1 = _f(row.get("milk_seq_cycle_right_1"))
    milk_seq_cycle_right_2 = _f(row.get("milk_seq_cycle_right_2"))
    milk_temp_left_1 = _f(row.get("milk_temp_left_1"))
    milk_temp_left_2 = _f(row.get("milk_temp_left_2"))
    milk_temp_right_1 = _f(row.get("milk_temp_right_1"))
    milk_temp_right_2 = _f(row.get("milk_temp_right_2"))
    milk_rpm_left_1 = _f(row.get("milk_rpm_left_1"))
    milk_rpm_left_2 = _f(row.get("milk_rpm_left_2"))
    milk_rpm_right_1 = _f(row.get("milk_rpm_right_1"))
    milk_rpm_right_2 = _f(row.get("milk_rpm_right_2"))

    machine_type = _derive_machine_type(row)
    milk_machine_type = _derive_milk_machine_type(row)

    hash_payload = {
        "machine_id": machine_id,
        "timestamp_start": ts_start.isoformat() if ts_start else None,
        "timestamp_end": ts_end.isoformat() if ts_end else None,
        "powder_clean_status": _i(row.get("powder_clean_status")),
        "tabs_status_left": _i(row.get("tabs_status_left")),
        "tabs_status_right": _i(row.get("tabs_status_right")),
        "detergent_status_left": _i(row.get("detergent_status_left")),
        "detergent_status_right": _i(row.get("detergent_status_right")),
        "milk_pump_error_left": _normalize_for_hash(milk_pump_error_left),
        "milk_pump_error_right": _normalize_for_hash(milk_pump_error_right),
        "milk_seq_cycle_left_1": _normalize_for_hash(milk_seq_cycle_left_1),
        "milk_seq_cycle_left_2": _normalize_for_hash(milk_seq_cycle_left_2),
        "milk_seq_cycle_right_1": _normalize_for_hash(milk_seq_cycle_right_1),
        "milk_seq_cycle_right_2": _normalize_for_hash(milk_seq_cycle_right_2),
        "milk_temp_left_1": _normalize_for_hash(milk_temp_left_1),
        "milk_temp_left_2": _normalize_for_hash(milk_temp_left_2),
        "milk_temp_right_1": _normalize_for_hash(milk_temp_right_1),
        "milk_temp_right_2": _normalize_for_hash(milk_temp_right_2),
        "milk_rpm_left_1": _normalize_for_hash(milk_rpm_left_1),
        "milk_rpm_left_2": _normalize_for_hash(milk_rpm_left_2),
        "milk_rpm_right_1": _normalize_for_hash(milk_rpm_right_1),
        "milk_rpm_right_2": _normalize_for_hash(milk_rpm_right_2),
    }
    source_row_hash = build_row_hash(hash_payload)

    return (
        machine_key,
        date_key,
        time_key,
        powder_status_key,
        tabs_status_left_key,
        tabs_status_right_key,
        detergent_status_left_key,
        detergent_status_right_key,
        ts_start,
        ts_end,
        duration_sec,
        milk_pump_error_left,
        milk_pump_error_right,
        milk_seq_cycle_left_1,
        milk_seq_cycle_left_2,
        milk_seq_cycle_right_1,
        milk_seq_cycle_right_2,
        milk_temp_left_1,
        milk_temp_left_2,
        milk_temp_right_1,
        milk_temp_right_2,
        milk_rpm_left_1,
        milk_rpm_left_2,
        milk_rpm_right_1,
        milk_rpm_right_2,
        machine_type,
        milk_machine_type,
        source_file_path,
        source_row_number,
        source_row_hash,
    )



def _load_file(conn, csv_path: Path) -> int:
    rel_path = str(csv_path.relative_to(DEFAULT_SILVER_ROOT))
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    stage_rows = []
    for idx, row in df.iterrows():
        stage_tuple = _row_to_stage_tuple(conn, row, rel_path, idx + 1)
        if stage_tuple is not None:
            stage_rows.append(stage_tuple)

    if not stage_rows:
        return 0

    total = len(stage_rows)

    try:
        with conn.cursor() as cur:
            cur.execute(TEMP_TABLE_SQL)
            cur.fast_executemany = True

            for start in range(0, total, BATCH_SIZE):
                cur.executemany(INSERT_STAGE_SQL, stage_rows[start:start + BATCH_SIZE])

            cur.execute(INSERT_FINAL_SQL)

        conn.commit()
        return total

    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(DROP_TEMP_SQL)
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass


def main() -> None:
    log.info("=== START gold load: fact_cleaning ===")
    log.info("Silver root: %s", DEFAULT_SILVER_ROOT)

    state = load_state(STATE_FILE, log)
    new_files = list_new_silver_files(CATEGORY, state)
    log.info("Silver files to load: %d", len(new_files))

    if not new_files:
        log.info("Gold layer up to date.")
        save_state(STATE_FILE, state)
        return

    errors = 0
    conn = None

    try:
        conn = get_connection()

        for csv_path in new_files:
            rel_path = str(csv_path.relative_to(DEFAULT_SILVER_ROOT))
            try:
                log.info("Loading %s ...", rel_path)
                n = _load_file(conn, csv_path)
                log.info("[OK] %s — %d row(s)", rel_path, n)
                state["loaded_files"].append(rel_path)
                save_state(STATE_FILE, state)
            except Exception as exc:
                errors += 1
                log.error("[ERROR] %s — %s", rel_path, exc)
                try:
                    conn.rollback()
                except Exception:
                    pass

    finally:
        if conn:
            conn.close()
        save_state(STATE_FILE, state)

    log.info("=== END gold load: fact_cleaning | errors=%d ===", errors)
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()