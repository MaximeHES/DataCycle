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
    get_rinse_type_key,
    get_flow_status_key,
    _f,
    _i,
    _dt,
    _s,
)

CATEGORY = "Rinse_History"
STATE_FILE = DEFAULT_GOLD_STATE_DIR / "rinse_state.json"
BATCH_SIZE = 500
log = setup_logger(__name__, "gold_rinse")

TEMP_TABLE_SQL = """
IF OBJECT_ID('tempdb..#stg_fact_rinse') IS NOT NULL
    DROP TABLE #stg_fact_rinse;

CREATE TABLE #stg_fact_rinse (
    machine_key INT NOT NULL,
    date_key INT NOT NULL,
    time_key INT NOT NULL,
    rinse_type_key INT NULL,
    flow_status_left_key INT NULL,
    flow_status_right_key INT NULL,
    nozzle_status_left_key INT NULL,
    nozzle_status_right_key INT NULL,
    source_timestamp DATETIME2(0) NOT NULL,
    flow_rate_left FLOAT NULL,
    flow_rate_right FLOAT NULL,
    pump_pressure_bar FLOAT NULL,
    nozzle_flow_rate_left FLOAT NULL,
    nozzle_flow_rate_right FLOAT NULL,
    side_active NVARCHAR(10) NULL,
    source_file_path NVARCHAR(500) NULL,
    source_row_number INT NOT NULL,
    source_row_hash CHAR(64) NOT NULL
);
"""

DROP_TEMP_SQL = """
IF OBJECT_ID('tempdb..#stg_fact_rinse') IS NOT NULL
    DROP TABLE #stg_fact_rinse;
"""

INSERT_STAGE_SQL = """
INSERT INTO #stg_fact_rinse (
    machine_key,
    date_key,
    time_key,
    rinse_type_key,
    flow_status_left_key,
    flow_status_right_key,
    nozzle_status_left_key,
    nozzle_status_right_key,
    source_timestamp,
    flow_rate_left,
    flow_rate_right,
    pump_pressure_bar,
    nozzle_flow_rate_left,
    nozzle_flow_rate_right,
    side_active,
    source_file_path,
    source_row_number,
    source_row_hash
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

INSERT_FINAL_SQL = """
INSERT INTO gold.fact_rinse (
    machine_key,
    date_key,
    time_key,
    rinse_type_key,
    flow_status_left_key,
    flow_status_right_key,
    nozzle_status_left_key,
    nozzle_status_right_key,
    source_timestamp,
    flow_rate_left,
    flow_rate_right,
    pump_pressure_bar,
    nozzle_flow_rate_left,
    nozzle_flow_rate_right,
    side_active,
    source_file_path,
    source_row_number,
    source_row_hash
)
SELECT
    s.machine_key,
    s.date_key,
    s.time_key,
    s.rinse_type_key,
    s.flow_status_left_key,
    s.flow_status_right_key,
    s.nozzle_status_left_key,
    s.nozzle_status_right_key,
    s.source_timestamp,
    s.flow_rate_left,
    s.flow_rate_right,
    s.pump_pressure_bar,
    s.nozzle_flow_rate_left,
    s.nozzle_flow_rate_right,
    s.side_active,
    s.source_file_path,
    s.source_row_number,
    s.source_row_hash
FROM #stg_fact_rinse s
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.fact_rinse f
    WHERE f.source_row_hash = s.source_row_hash
);
"""


def build_row_hash(record: dict) -> str:
    payload = json.dumps(record, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _normalize_for_hash(value):
    if value is None:
        return None
    if isinstance(value, float):
        return round(value, 8)
    return value


def _sanitize_metric(val):
    v = _f(val)
    if v is None:
        return None
    if v == 65535:
        return 0.0
    return v


def _sanitize_status_code(val):
    v = _i(val)
    if v is None:
        return None
    return v


def _derive_side_active(row: pd.Series) -> str | None:
    left = _sanitize_metric(row.get("flow_rate_left"))
    right = _sanitize_metric(row.get("flow_rate_right"))
    if left is None and right is None:
        return None
    left_on = (left or 0) > 0
    right_on = (right or 0) > 0
    if left_on and right_on:
        return "both"
    if left_on:
        return "left"
    if right_on:
        return "right"
    return "none"


def _row_to_stage_tuple(conn, row: pd.Series, source_file_path: str, source_row_number: int):
    ts = _dt(row.get("timestamp"))
    machine_id = _i(row.get("machine_id"))

    if ts is None or machine_id is None:
        return None

    machine_key = get_or_create_machine(conn, machine_id)
    date_key = get_date_key(conn, ts)
    time_key = get_time_key(ts)

    rinse_type_key = get_rinse_type_key(conn, row.get("rinse_type"))
    flow_status_left_key = get_flow_status_key(conn, _sanitize_status_code(row.get("status_left")))
    flow_status_right_key = get_flow_status_key(conn, _sanitize_status_code(row.get("status_right")))
    nozzle_status_left_key = get_flow_status_key(conn, _sanitize_status_code(row.get("nozzle_status_left")))
    nozzle_status_right_key = get_flow_status_key(conn, _sanitize_status_code(row.get("nozzle_status_right")))

    flow_rate_left = _sanitize_metric(row.get("flow_rate_left"))
    flow_rate_right = _sanitize_metric(row.get("flow_rate_right"))
    pump_pressure_bar = _sanitize_metric(row.get("pump_pressure"))
    nozzle_flow_rate_left = _sanitize_metric(row.get("nozzle_flow_rate_left"))
    nozzle_flow_rate_right = _sanitize_metric(row.get("nozzle_flow_rate_right"))
    side_active = _s(row.get("side_active")) or _derive_side_active(row)

    hash_payload = {
        "machine_id": machine_id,
        "timestamp": ts.isoformat(),
        "rinse_type": _i(row.get("rinse_type")),
        "status_left": _sanitize_status_code(row.get("status_left")),
        "status_right": _sanitize_status_code(row.get("status_right")),
        "nozzle_status_left": _sanitize_status_code(row.get("nozzle_status_left")),
        "nozzle_status_right": _sanitize_status_code(row.get("nozzle_status_right")),
        "flow_rate_left": _normalize_for_hash(flow_rate_left),
        "flow_rate_right": _normalize_for_hash(flow_rate_right),
        "pump_pressure_bar": _normalize_for_hash(pump_pressure_bar),
        "nozzle_flow_rate_left": _normalize_for_hash(nozzle_flow_rate_left),
        "nozzle_flow_rate_right": _normalize_for_hash(nozzle_flow_rate_right),
        "side_active": side_active,
    }
    source_row_hash = build_row_hash(hash_payload)

    return (
        machine_key,
        date_key,
        time_key,
        rinse_type_key,
        flow_status_left_key,
        flow_status_right_key,
        nozzle_status_left_key,
        nozzle_status_right_key,
        ts,
        flow_rate_left,
        flow_rate_right,
        pump_pressure_bar,
        nozzle_flow_rate_left,
        nozzle_flow_rate_right,
        side_active,
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
    log.info("=== START gold load: fact_rinse ===")
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

    log.info("=== END gold load: fact_rinse | errors=%d ===", errors)
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()