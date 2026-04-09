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
    get_alert_type_key,
    _v,
    _i,
    _dt,
)

CATEGORY = "Info_Message_History"
STATE_FILE = DEFAULT_GOLD_STATE_DIR / "alerts_state.json"
BATCH_SIZE = 500
log = setup_logger(__name__, "gold_alerts")

TEMP_TABLE_SQL = """
IF OBJECT_ID('tempdb..#stg_fact_alerts') IS NOT NULL
    DROP TABLE #stg_fact_alerts;

CREATE TABLE #stg_fact_alerts (
    machine_key INT NOT NULL,
    date_key INT NOT NULL,
    time_key INT NOT NULL,
    alert_type_key INT NULL,
    source_timestamp DATETIME2(0) NOT NULL,
    source_file_path NVARCHAR(500) NULL,
    source_row_number INT NOT NULL,
    source_row_hash CHAR(64) NOT NULL
);
"""

DROP_TEMP_SQL = """
IF OBJECT_ID('tempdb..#stg_fact_alerts') IS NOT NULL
    DROP TABLE #stg_fact_alerts;
"""

INSERT_STAGE_SQL = """
INSERT INTO #stg_fact_alerts (
    machine_key,
    date_key,
    time_key,
    alert_type_key,
    source_timestamp,
    source_file_path,
    source_row_number,
    source_row_hash
) VALUES (?,?,?,?,?,?,?,?)
"""

INSERT_FINAL_SQL = """
INSERT INTO gold.fact_alerts (
    machine_key,
    date_key,
    time_key,
    alert_type_key,
    source_timestamp,
    source_file_path,
    source_row_number,
    source_row_hash
)
SELECT
    s.machine_key,
    s.date_key,
    s.time_key,
    s.alert_type_key,
    s.source_timestamp,
    s.source_file_path,
    s.source_row_number,
    s.source_row_hash
FROM #stg_fact_alerts s
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.fact_alerts f
    WHERE f.source_row_hash = s.source_row_hash
);
"""


def build_row_hash(record: dict) -> str:
    payload = json.dumps(record, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _row_to_stage_tuple(conn, row: pd.Series, source_file_path: str, source_row_number: int):
    ts = _dt(row.get("timestamp"))
    machine_id = _i(row.get("machine_id"))

    if ts is None or machine_id is None:
        return None

    alert_code = _v(row.get("number"))
    typography = _v(row.get("typography")) or _v(row.get("message_prefix"))
    type_number = _i(row.get("type_number")) or _i(row.get("message_code"))

    machine_key = get_or_create_machine(conn, machine_id)
    date_key = get_date_key(conn, ts)
    time_key = get_time_key(ts)
    alert_type_key = get_alert_type_key(conn, alert_code, typography, type_number)

    hash_payload = {
        "machine_id": machine_id,
        "timestamp": ts.isoformat(),
        "alert_code": str(alert_code) if alert_code is not None else None,
        "typography": str(typography) if typography is not None else None,
        "type_number": type_number,
    }
    source_row_hash = build_row_hash(hash_payload)

    return (
        machine_key,
        date_key,
        time_key,
        alert_type_key,
        ts,
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
    log.info("=== START gold load: fact_alerts ===")
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

    log.info("=== END gold load: fact_alerts | errors=%d ===", errors)
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()