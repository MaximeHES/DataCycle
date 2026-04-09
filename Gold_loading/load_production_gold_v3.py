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
    get_product_key,
    get_hopper_key,
    get_stop_key,
    _f,
    _i,
    _dt,
)

CATEGORY = "Product_History"
STATE_FILE = DEFAULT_GOLD_STATE_DIR / "production_state.json"
BATCH_SIZE = 500
log = setup_logger(__name__, "gold_production")

TEMP_TABLE_SQL = """
IF OBJECT_ID('tempdb..#stg_fact_production') IS NOT NULL
    DROP TABLE #stg_fact_production;

CREATE TABLE #stg_fact_production (
    machine_key INT NOT NULL,
    date_key INT NOT NULL,
    time_key INT NOT NULL,
    product_key INT NULL,
    hopper_key INT NULL,
    stop_key INT NULL,
    source_timestamp DATETIME2(0) NOT NULL,
    outlet_side TINYINT NULL,
    is_double BIT NULL,
    press_before FLOAT NULL,
    press_after FLOAT NULL,
    press_final FLOAT NULL,
    grind_time_sec FLOAT NULL,
    extraction_time_sec FLOAT NULL,
    milk_time_sec FLOAT NULL,
    water_qnty_ticks FLOAT NULL,
    water_temp_c FLOAT NULL,
    milk_temp_c FLOAT NULL,
    boiler_temp_c FLOAT NULL,
    steam_pressure_bar FLOAT NULL,
    grind_adjust_right FLOAT NULL,
    grind_adjust_left FLOAT NULL,
    is_coffee_extraction BIT NULL,
    is_milk_event BIT NULL,
    source_file_path NVARCHAR(500) NULL,
    source_row_number INT NOT NULL,
    source_row_hash CHAR(64) NOT NULL
);
"""

DROP_TEMP_SQL = """
IF OBJECT_ID('tempdb..#stg_fact_production') IS NOT NULL
    DROP TABLE #stg_fact_production;
"""

INSERT_STAGE_SQL = """
INSERT INTO #stg_fact_production (
    machine_key,
    date_key,
    time_key,
    product_key,
    hopper_key,
    stop_key,
    source_timestamp,
    outlet_side,
    is_double,
    press_before,
    press_after,
    press_final,
    grind_time_sec,
    extraction_time_sec,
    milk_time_sec,
    water_qnty_ticks,
    water_temp_c,
    milk_temp_c,
    boiler_temp_c,
    steam_pressure_bar,
    grind_adjust_right,
    grind_adjust_left,
    is_coffee_extraction,
    is_milk_event,
    source_file_path,
    source_row_number,
    source_row_hash
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

INSERT_FINAL_SQL = """
INSERT INTO gold.fact_production (
    machine_key,
    date_key,
    time_key,
    product_key,
    hopper_key,
    stop_key,
    source_timestamp,
    outlet_side,
    is_double,
    press_before,
    press_after,
    press_final,
    grind_time_sec,
    extraction_time_sec,
    milk_time_sec,
    water_qnty_ticks,
    water_temp_c,
    milk_temp_c,
    boiler_temp_c,
    steam_pressure_bar,
    grind_adjust_right,
    grind_adjust_left,
    is_coffee_extraction,
    is_milk_event,
    source_file_path,
    source_row_number,
    source_row_hash
)
SELECT
    s.machine_key,
    s.date_key,
    s.time_key,
    s.product_key,
    s.hopper_key,
    s.stop_key,
    s.source_timestamp,
    s.outlet_side,
    s.is_double,
    s.press_before,
    s.press_after,
    s.press_final,
    s.grind_time_sec,
    s.extraction_time_sec,
    s.milk_time_sec,
    s.water_qnty_ticks,
    s.water_temp_c,
    s.milk_temp_c,
    s.boiler_temp_c,
    s.steam_pressure_bar,
    s.grind_adjust_right,
    s.grind_adjust_left,
    s.is_coffee_extraction,
    s.is_milk_event,
    s.source_file_path,
    s.source_row_number,
    s.source_row_hash
FROM #stg_fact_production s
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.fact_production f
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


def _row_to_stage_tuple(conn, row: pd.Series, source_file_path: str, source_row_number: int):
    ts = _dt(row.get("timestamp"))
    machine_id = _i(row.get("machine_id"))

    if ts is None or machine_id is None:
        return None

    machine_key = get_or_create_machine(conn, machine_id)
    date_key = get_date_key(conn, ts)
    time_key = get_time_key(ts)
    product_key = get_product_key(conn, row.get("prod_type"))
    hopper_key = get_hopper_key(conn, row.get("bean_hopper"))
    stop_key = get_stop_key(conn, row.get("stopped"))

    is_double = _i(row.get("is_double") if "is_double" in row.index else row.get("double_prod"))

    press_before = _f(row.get("press_before"))
    press_after = _f(row.get("press_after"))
    press_final = _f(row.get("press_final"))
    grind_time_sec = _f(row.get("grind_time"))
    extraction_time_sec = _f(row.get("ext_time"))
    milk_time_sec = _f(row.get("milk_time"))
    water_qnty_ticks = _f(row.get("water_qnty"))
    water_temp_c = _f(row.get("water_temp"))
    milk_temp_c = _f(row.get("milk_temp"))
    boiler_temp_c = _f(row.get("boiler_temp"))
    steam_pressure_bar = _f(row.get("steam_pressure"))
    grind_adjust_right = _f(row.get("grind_adjust_right"))
    grind_adjust_left = _f(row.get("grind_adjust_left"))
    is_coffee_extraction = _i(row.get("is_coffee_extraction"))
    is_milk_event = _i(row.get("is_milk_event"))
    outlet_side = _i(row.get("outlet_side"))

    hash_payload = {
        "machine_id": machine_id,
        "timestamp": ts.isoformat(),
        "prod_type": _i(row.get("prod_type")),
        "bean_hopper": _i(row.get("bean_hopper")),
        "stopped": _i(row.get("stopped")),
        "outlet_side": outlet_side,
        "is_double": is_double,
        "press_before": _normalize_for_hash(press_before),
        "press_after": _normalize_for_hash(press_after),
        "press_final": _normalize_for_hash(press_final),
        "grind_time_sec": _normalize_for_hash(grind_time_sec),
        "extraction_time_sec": _normalize_for_hash(extraction_time_sec),
        "milk_time_sec": _normalize_for_hash(milk_time_sec),
        "water_qnty_ticks": _normalize_for_hash(water_qnty_ticks),
        "water_temp_c": _normalize_for_hash(water_temp_c),
        "milk_temp_c": _normalize_for_hash(milk_temp_c),
        "boiler_temp_c": _normalize_for_hash(boiler_temp_c),
        "steam_pressure_bar": _normalize_for_hash(steam_pressure_bar),
        "grind_adjust_right": _normalize_for_hash(grind_adjust_right),
        "grind_adjust_left": _normalize_for_hash(grind_adjust_left),
        "is_coffee_extraction": is_coffee_extraction,
        "is_milk_event": is_milk_event,
    }
    source_row_hash = build_row_hash(hash_payload)

    return (
        machine_key,
        date_key,
        time_key,
        product_key,
        hopper_key,
        stop_key,
        ts,
        outlet_side,
        is_double,
        press_before,
        press_after,
        press_final,
        grind_time_sec,
        extraction_time_sec,
        milk_time_sec,
        water_qnty_ticks,
        water_temp_c,
        milk_temp_c,
        boiler_temp_c,
        steam_pressure_bar,
        grind_adjust_right,
        grind_adjust_left,
        is_coffee_extraction,
        is_milk_event,
        source_file_path,
        source_row_number,
        source_row_hash,
    )


def _load_file(conn, csv_path: Path) -> int:
    rel_path = str(csv_path.relative_to(DEFAULT_SILVER_ROOT))
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    stage_rows = []
    for idx, row in df.iterrows():
        stage_tuple = _row_to_stage_tuple(
            conn=conn,
            row=row,
            source_file_path=rel_path,
            source_row_number=idx + 1,
        )
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
    log.info("=== START gold load: fact_production ===")
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
        # DB already initialized manually
        # ensure_schema(conn)

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

    log.info("=== END gold load: fact_production | errors=%d ===", errors)
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()