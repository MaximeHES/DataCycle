from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional
from db_config import DB_CONFIG

import pyodbc


DEFAULT_SILVER_ROOT = Path(os.environ.get("EVERSYS_SILVER_ROOT", r"C:\RawData\Eversys_Cleaned"))
DEFAULT_GOLD_STATE_DIR = Path(
    os.environ.get("EVERSYS_GOLD_STATE_DIR", str(DEFAULT_SILVER_ROOT / "_state" / "gold"))
)
DEFAULT_GOLD_LOG_DIR = Path(
    os.environ.get("EVERSYS_GOLD_LOG_DIR", str(DEFAULT_SILVER_ROOT / "_logs" / "Eversys_Gold"))
)
SQL_DIR = Path(__file__).resolve().parent / "sql"


# ---------------------------------------------------------------------------
# Connection / logging / state
# ---------------------------------------------------------------------------

def get_connection():
    import pyodbc

    conn_str = (
        f"DRIVER={{{DB_CONFIG['driver']}}};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['username']};"
        f"PWD={DB_CONFIG['password']};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )

    return pyodbc.connect(conn_str)


def setup_logger(name: str, prefix: str) -> logging.Logger:
    DEFAULT_GOLD_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger(name)
    if log.handlers:
        return log

    log.setLevel(logging.INFO)
    formatter = logging.Formatter(
        fmt="[%(asctime)s] %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(
        DEFAULT_GOLD_LOG_DIR / f"{prefix}_{datetime.now():%Y%m%d_%H%M%S}.log",
        encoding="utf-8",
    )
    fh.setFormatter(formatter)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)

    log.addHandler(fh)
    log.addHandler(sh)
    return log


def load_state(state_file: Path, log: logging.Logger) -> dict[str, Any]:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    if not state_file.exists():
        return {"loaded_files": [], "last_run_utc": None}
    try:
        return json.loads(state_file.read_text(encoding="utf-8"))
    except Exception as exc:
        log.warning("State unreadable (%s) — starting fresh.", exc)
        return {"loaded_files": [], "last_run_utc": None}


def save_state(state_file: Path, state: dict[str, Any]) -> None:
    state["last_run_utc"] = datetime.utcnow().isoformat()
    tmp = state_file.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(state_file)


def list_new_silver_files(category: str, state: dict[str, Any]) -> list[Path]:
    silver_dir = DEFAULT_SILVER_ROOT / category
    if not silver_dir.exists():
        return []
    loaded = set(state.get("loaded_files", []))
    all_files = sorted(silver_dir.rglob("*_CLEANED.csv"))
    return [p for p in all_files if str(p.relative_to(DEFAULT_SILVER_ROOT)) not in loaded]


# ---------------------------------------------------------------------------
# SQL bootstrap
# ---------------------------------------------------------------------------

def _split_sql_batches(script_text: str) -> list[str]:
    lines = script_text.splitlines()
    batches: list[list[str]] = [[]]
    for line in lines:
        if line.strip().upper() == "GO":
            if batches[-1]:
                batches.append([])
            continue
        batches[-1].append(line)
    return ["\n".join(batch).strip() for batch in batches if "\n".join(batch).strip()]


def execute_sql_script(conn: pyodbc.Connection, script_path: Path) -> None:
    script = script_path.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        for batch in _split_sql_batches(script):
            cur.execute(batch)
    conn.commit()


def ensure_schema(conn: pyodbc.Connection) -> None:
    for name in [
        "001_gold_schema_v3.sql",
        "002_gold_seed_dimensions_v3.sql",
        "003_gold_reporting_views_v3.sql",
    ]:
        execute_sql_script(conn, SQL_DIR / name)


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------

def _v(val: Any) -> Any:
    if val is None:
        return None
    try:
        import pandas as pd
        if pd.isna(val):
            return None
    except Exception:
        pass
    if isinstance(val, str) and val.strip().lower() in {"", "nan", "none", "null", "<na>"}:
        return None
    return val


def _f(val: Any) -> Optional[float]:
    v = _v(val)
    return float(v) if v is not None else None


def _i(val: Any) -> Optional[int]:
    v = _v(val)
    return int(float(v)) if v is not None else None


def _s(val: Any) -> Optional[str]:
    v = _v(val)
    return str(v).strip() if v is not None else None


def _dt(val: Any) -> Optional[datetime]:
    import pandas as pd

    v = _v(val)
    if v is None:
        return None
    try:
        ts = pd.to_datetime(v, errors="coerce")
        return None if pd.isna(ts) else ts.to_pydatetime().replace(tzinfo=None)
    except Exception:
        return None


def _normalize_flow_value(val: Any) -> Optional[float]:
    f = _f(val)
    if f is None:
        return None
    if f in (65535.0, 65535):
        return None
    return f


# ---------------------------------------------------------------------------
# Business mappings
# ---------------------------------------------------------------------------

PRODUCT_CATALOG: dict[int, dict[str, Any]] = {
    0: {"product_name": "None", "coffee_amount": 0, "water_amount": 0, "milk_amount": 0, "category": "None", "has_milk": 0, "has_coffee": 0, "is_hot_water": 0, "is_steam": 0},
    1: {"product_name": "Ristretto", "coffee_amount": 7, "water_amount": 40, "milk_amount": 0, "category": "Coffee", "has_milk": 0, "has_coffee": 1, "is_hot_water": 0, "is_steam": 0},
    2: {"product_name": "Espresso", "coffee_amount": 7, "water_amount": 70, "milk_amount": 0, "category": "Coffee", "has_milk": 0, "has_coffee": 1, "is_hot_water": 0, "is_steam": 0},
    3: {"product_name": "Coffee", "coffee_amount": 10, "water_amount": 90, "milk_amount": 0, "category": "Coffee", "has_milk": 0, "has_coffee": 1, "is_hot_water": 0, "is_steam": 0},
    4: {"product_name": "Filter Coffee", "coffee_amount": 12, "water_amount": 90, "milk_amount": 0, "category": "Coffee", "has_milk": 0, "has_coffee": 1, "is_hot_water": 0, "is_steam": 0},
    5: {"product_name": "Americano", "coffee_amount": 7, "water_amount": 310, "milk_amount": 0, "category": "Coffee", "has_milk": 0, "has_coffee": 1, "is_hot_water": 0, "is_steam": 0},
    6: {"product_name": "Coffee Pot", "coffee_amount": 50, "water_amount": 600, "milk_amount": 0, "category": "Coffee", "has_milk": 0, "has_coffee": 1, "is_hot_water": 0, "is_steam": 0},
    7: {"product_name": "Filter Coffee Pot", "coffee_amount": 60, "water_amount": 600, "milk_amount": 0, "category": "Coffee", "has_milk": 0, "has_coffee": 1, "is_hot_water": 0, "is_steam": 0},
    8: {"product_name": "Hot Water", "coffee_amount": 0, "water_amount": 90, "milk_amount": 0, "category": "Water", "has_milk": 0, "has_coffee": 0, "is_hot_water": 1, "is_steam": 0},
    9: {"product_name": "Manual Steam", "coffee_amount": 0, "water_amount": 0, "milk_amount": 0, "category": "Steam", "has_milk": 0, "has_coffee": 0, "is_hot_water": 0, "is_steam": 1},
    10: {"product_name": "Auto Steam", "coffee_amount": 0, "water_amount": 0, "milk_amount": 0, "category": "Steam", "has_milk": 0, "has_coffee": 0, "is_hot_water": 0, "is_steam": 1},
    11: {"product_name": "Everfoam", "coffee_amount": 0, "water_amount": 0, "milk_amount": 0, "category": "Milk", "has_milk": 1, "has_coffee": 0, "is_hot_water": 0, "is_steam": 0},
    12: {"product_name": "Milk Coffee", "coffee_amount": 7, "water_amount": 90, "milk_amount": 90, "category": "Milk Coffee", "has_milk": 1, "has_coffee": 1, "is_hot_water": 0, "is_steam": 0},
    13: {"product_name": "Cappuccino", "coffee_amount": 7, "water_amount": 70, "milk_amount": 50, "category": "Milk Coffee", "has_milk": 1, "has_coffee": 1, "is_hot_water": 0, "is_steam": 0},
    14: {"product_name": "Espresso Macchiato", "coffee_amount": 7, "water_amount": 50, "milk_amount": 50, "category": "Milk Coffee", "has_milk": 1, "has_coffee": 1, "is_hot_water": 0, "is_steam": 0},
    15: {"product_name": "Latte Macchiato", "coffee_amount": 7, "water_amount": 60, "milk_amount": 150, "category": "Milk Coffee", "has_milk": 1, "has_coffee": 1, "is_hot_water": 0, "is_steam": 0},
    16: {"product_name": "Milk", "coffee_amount": 0, "water_amount": 0, "milk_amount": 200, "category": "Milk", "has_milk": 1, "has_coffee": 0, "is_hot_water": 0, "is_steam": 0},
    17: {"product_name": "Milk Foam", "coffee_amount": 0, "water_amount": 0, "milk_amount": 180, "category": "Milk", "has_milk": 1, "has_coffee": 0, "is_hot_water": 0, "is_steam": 0},
    18: {"product_name": "Powder", "coffee_amount": 0, "water_amount": 0, "milk_amount": 0, "category": "Powder", "has_milk": 0, "has_coffee": 0, "is_hot_water": 0, "is_steam": 0},
    19: {"product_name": "White Americano", "coffee_amount": 7, "water_amount": 100, "milk_amount": 20, "category": "Milk Coffee", "has_milk": 1, "has_coffee": 1, "is_hot_water": 0, "is_steam": 0},
    255: {"product_name": "Undef", "coffee_amount": 0, "water_amount": 0, "milk_amount": 0, "category": "Unknown", "has_milk": 0, "has_coffee": 0, "is_hot_water": 0, "is_steam": 0},
}

STOP_REASON_MAP = {
    0: {"stop_reason": "Finished", "is_error": 0},
    1: {"stop_reason": "Stopped", "is_error": 0},
    2: {"stop_reason": "Machine Abort", "is_error": 1},
    3: {"stop_reason": "User Abort", "is_error": 0},
}

RINSE_TYPE_MAP = {
    0: {"rinse_name": "InitReboot", "description": "Rinse triggered at reboot"},
    1: {"rinse_name": "InitWakeup", "description": "Rinse triggered at wakeup"},
    2: {"rinse_name": "WarmLeft", "description": "Warm-up rinse left"},
    3: {"rinse_name": "WarmRight", "description": "Warm-up rinse right"},
    4: {"rinse_name": "AfterClean", "description": "Rinse after cleaning"},
    5: {"rinse_name": "FlowRate", "description": "Flow rate rinse"},
    6: {"rinse_name": "RequestedEtc", "description": "Requested or other rinse"},
    7: {"rinse_name": "Max", "description": "Technical max code"},
    255: {"rinse_name": "Undef", "description": "Undefined rinse type"},
}

FLOW_STATUS_MAP = {
    0: {"status_name": "Undef", "is_ok": 0},
    1: {"status_name": "Unknown", "is_ok": 0},
    2: {"status_name": "TooLow", "is_ok": 0},
    3: {"status_name": "TooHigh", "is_ok": 0},
    4: {"status_name": "Nozzle05", "is_ok": 1},
    5: {"status_name": "Nozzle07", "is_ok": 1},
    6: {"status_name": "SystemOk", "is_ok": 1},
    255: {"status_name": "Undef", "is_ok": 0},
}

TABS_STATUS_MAP = {
    0: {"status_name": "No", "is_ok": 0},
    1: {"status_name": "Yes", "is_ok": 1},
    2: {"status_name": "Undef", "is_ok": 0},
    3: {"status_name": "Error", "is_ok": 0},
    4: {"status_name": "Unknown", "is_ok": 0},
    5: {"status_name": "NotNecessary", "is_ok": 1},
    6: {"status_name": "CycleError", "is_ok": 0},
    7: {"status_name": "Max", "is_ok": 0},
}

DETERGENT_STATUS_MAP = {
    0: {"status_name": "Undef", "is_ok": 0},
    1: {"status_name": "No", "is_ok": 0},
    2: {"status_name": "Yes", "is_ok": 1},
    3: {"status_name": "Error", "is_ok": 0},
    4: {"status_name": "Unknown", "is_ok": 0},
    5: {"status_name": "NotNecessary", "is_ok": 1},
    6: {"status_name": "CycleAbort", "is_ok": 0},
    7: {"status_name": "CycleWarning", "is_ok": 0},
    8: {"status_name": "DetergentWarning", "is_ok": 0},
    9: {"status_name": "Max", "is_ok": 0},
}

POWDER_STATUS_MAP = {
    0: {"status_name": "Undef", "reserved": None},
    1: {"status_name": "NotNecessary", "reserved": None},
    2: {"status_name": "MixerCleaned", "reserved": None},
    3: {"status_name": "WithoutMixer", "reserved": None},
    4: {"status_name": "Max", "reserved": None},
}

ALERT_CATALOG = {
    "E-000": {"info_message": "Bean hopper rear missing.", "typography": "E", "type_number": 0, "severity": "Low", "module": "Bean hopper", "is_visible": 1, "is_recurring": 0, "is_logged": 1},
    "E-001": {"info_message": "Bean hopper front missing.", "typography": "E", "type_number": 1, "severity": "Low", "module": "Bean hopper", "is_visible": 1, "is_recurring": 0, "is_logged": 1},
    "E-010": {"info_message": "Software too old. Please start software update.", "typography": "E", "type_number": 10, "severity": "Critical", "module": "CPU", "is_visible": 1, "is_recurring": 0, "is_logged": 1},
    "W-013": {"info_message": "Service necessary.", "typography": "W", "type_number": 13, "severity": "Medium", "module": "Machine", "is_visible": 0, "is_recurring": 1, "is_logged": 1},
    "W-014": {"info_message": "Please change the water filter.", "typography": "W", "type_number": 14, "severity": "Medium", "module": "Water supply", "is_visible": 0, "is_recurring": 1, "is_logged": 1},
    "S-017": {"info_message": "Cleaning necessary. Please press Continue to start the cleaning.", "typography": "S", "type_number": 17, "severity": "Low", "module": "Display / Touch screen", "is_visible": 1, "is_recurring": 0, "is_logged": 1},
    "S-018": {"info_message": "The last cleaning hasn't been finished correctly. Please press Continue to start the cleaning.", "typography": "S", "type_number": 18, "severity": "Low", "module": "Display / Touch screen", "is_visible": 1, "is_recurring": 0, "is_logged": 1},
}


# ---------------------------------------------------------------------------
# Lookup helpers
# ---------------------------------------------------------------------------

_date_cache: dict[int, int] = {}
_machine_cache: dict[int, int] = {}
_dim_cache: dict[tuple[str, Any], int] = {}


def get_date_key(conn: pyodbc.Connection, dt: datetime | date) -> int:
    d = dt.date() if isinstance(dt, datetime) else dt
    key = int(d.strftime("%Y%m%d"))
    if key in _date_cache:
        return key

    with conn.cursor() as cur:
        cur.execute("SELECT date_key FROM gold.dim_date WHERE date_key = ?", key)
        if cur.fetchone():
            _date_cache[key] = key
            return key
        iso = d.isocalendar()
        cur.execute(
            """
            INSERT INTO gold.dim_date
              (date_key, full_date, year, month, month_name, quarter, week_iso,
               day_of_month, day_of_week, day_name, is_weekend)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            key, d, d.year, d.month, d.strftime("%B"), (d.month - 1) // 3 + 1,
            iso[1], d.day, iso[2], d.strftime("%A"), 1 if iso[2] >= 6 else 0,
        )
    conn.commit()
    _date_cache[key] = key
    return key


def get_time_key(dt: datetime) -> int:
    return dt.hour * 100 + dt.minute


def get_or_create_machine(conn: pyodbc.Connection, machine_id: int) -> int:
    if machine_id in _machine_cache:
        return _machine_cache[machine_id]
    with conn.cursor() as cur:
        cur.execute(
            "SELECT machine_key FROM gold.dim_machine WHERE machine_id=? AND is_current=1",
            machine_id,
        )
        row = cur.fetchone()
        if row:
            _machine_cache[machine_id] = row[0]
            return row[0]
        cur.execute(
            """
            INSERT INTO gold.dim_machine
              (machine_id, machine_name, location, model, install_date,
               is_active, valid_from, valid_to, is_current)
            OUTPUT INSERTED.machine_key
            VALUES (?, NULL, NULL, NULL, NULL, 1, SYSUTCDATETIME(), NULL, 1)
            """,
            machine_id,
        )
        key = cur.fetchone()[0]
    conn.commit()
    _machine_cache[machine_id] = key
    return key


def _upsert_dim(conn: pyodbc.Connection, table: str, pk_col: str, nk_col: str, nk_value: Any, defaults: dict[str, Any] | None = None) -> Optional[int]:
    if nk_value is None:
        return None

    cache_key = (table, nk_value)
    if cache_key in _dim_cache:
        return _dim_cache[cache_key]

    defaults = defaults or {}
    with conn.cursor() as cur:
        cur.execute(f"SELECT [{pk_col}] FROM gold.[{table}] WHERE [{nk_col}] = ?", nk_value)
        row = cur.fetchone()
        if row:
            pk = row[0]
            if defaults:
                assignments = ", ".join(f"[{k}] = COALESCE([{k}], ?)" for k in defaults.keys())
                cur.execute(
                    f"UPDATE gold.[{table}] SET {assignments} WHERE [{nk_col}] = ?",
                    *defaults.values(), nk_value,
                )
            conn.commit()
            _dim_cache[cache_key] = pk
            return pk

        cols = [nk_col] + list(defaults.keys())
        vals = [nk_value] + list(defaults.values())
        placeholders = ", ".join("?" for _ in vals)
        col_list = ", ".join(f"[{c}]" for c in cols)
        cur.execute(
            f"INSERT INTO gold.[{table}] ({col_list}) OUTPUT INSERTED.[{pk_col}] VALUES ({placeholders})",
            *vals,
        )
        pk = cur.fetchone()[0]
    conn.commit()
    _dim_cache[cache_key] = pk
    return pk


def get_product_key(conn: pyodbc.Connection, prod_type: Any) -> Optional[int]:
    code = _i(prod_type)
    return _upsert_dim(conn, "dim_product_type", "product_key", "prod_type", code, PRODUCT_CATALOG.get(code))


def get_hopper_key(conn: pyodbc.Connection, bean_hopper: Any) -> Optional[int]:
    code = _i(bean_hopper)
    if code in (None, 255):
        return None
    defaults = {
        "hopper_name": f"Hopper {code}",
        "position": "Unknown",
    }
    return _upsert_dim(conn, "dim_bean_hopper", "hopper_key", "hopper_code", code, defaults)


def get_stop_key(conn: pyodbc.Connection, stopped: Any) -> Optional[int]:
    code = _i(stopped)
    return _upsert_dim(conn, "dim_stop_reason", "stop_key", "stop_code", code, STOP_REASON_MAP.get(code, {"stop_reason": f"Code {code}", "is_error": 0} if code is not None else None))


def get_rinse_type_key(conn: pyodbc.Connection, rinse_type: Any) -> Optional[int]:
    code = _i(rinse_type)
    return _upsert_dim(conn, "dim_rinse_type", "rinse_type_key", "rinse_code", code, RINSE_TYPE_MAP.get(code, {"rinse_name": f"Code {code}", "description": None} if code is not None else None))


def get_flow_status_key(conn: pyodbc.Connection, status_code: Any) -> Optional[int]:
    code = _i(status_code)
    return _upsert_dim(conn, "dim_flow_status", "status_key", "status_code", code, FLOW_STATUS_MAP.get(code, {"status_name": f"Code {code}", "is_ok": 0} if code is not None else None))


def get_tabs_status_key(conn: pyodbc.Connection, status_code: Any) -> Optional[int]:
    code = _i(status_code)
    return _upsert_dim(conn, "dim_tabs_status", "status_key", "status_code", code, TABS_STATUS_MAP.get(code, {"status_name": f"Code {code}", "is_ok": 0} if code is not None else None))


def get_detergent_status_key(conn: pyodbc.Connection, status_code: Any) -> Optional[int]:
    code = _i(status_code)
    return _upsert_dim(conn, "dim_detergent_status", "status_key", "status_code", code, DETERGENT_STATUS_MAP.get(code, {"status_name": f"Code {code}", "is_ok": 0} if code is not None else None))


def get_powder_status_key(conn: pyodbc.Connection, status_code: Any) -> Optional[int]:
    code = _i(status_code)
    return _upsert_dim(conn, "dim_powder_status", "status_key", "status_code", code, POWDER_STATUS_MAP.get(code, {"status_name": f"Code {code}", "reserved": None} if code is not None else None))


def get_alert_type_key(conn: pyodbc.Connection, alert_code: Any, typography: Any = None, type_number: Any = None) -> Optional[int]:
    code = _s(alert_code)
    if code is None:
        return None

    defaults = ALERT_CATALOG.get(code, {}).copy()
    if _s(typography) is not None:
        defaults.setdefault("typography", _s(typography))
    elif "typography" not in defaults and "-" in code:
        defaults["typography"] = code.split("-", 1)[0]

    if _i(type_number) is not None:
        defaults.setdefault("type_number", _i(type_number))
    elif "type_number" not in defaults and "-" in code:
        try:
            defaults["type_number"] = int(code.split("-", 1)[1])
        except Exception:
            pass

    if "severity" not in defaults:
        prefix = defaults.get("typography")
        defaults["severity"] = {"E": "High", "W": "Medium", "S": "Low"}.get(prefix, "Unknown")
    if "info_message" not in defaults:
        defaults["info_message"] = code

    return _upsert_dim(conn, "dim_alert_type", "alert_type_key", "alert_code", code, defaults)


# ---------------------------------------------------------------------------
# Idempotent row hashing / staging helpers
# ---------------------------------------------------------------------------

def build_row_hash(*parts: Any) -> str:
    normalized = [
        "" if p is None else (p.isoformat(sep=" ", timespec="seconds") if isinstance(p, datetime) else str(p))
        for p in parts
    ]
    return hashlib.sha256("||".join(normalized).encode("utf-8")).hexdigest()


def create_temp_table(conn: pyodbc.Connection, table_sql: str) -> None:
    with conn.cursor() as cur:
        cur.execute(table_sql)
    conn.commit()


def bulk_insert_temp(conn: pyodbc.Connection, insert_sql: str, rows: list[tuple[Any, ...]], batch_size: int = 1000) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.fast_executemany = True
        for start in range(0, len(rows), batch_size):
            cur.executemany(insert_sql, rows[start:start + batch_size])
    conn.commit()
