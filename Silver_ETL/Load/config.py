import os
from pathlib import Path

SILVER_ROOT = Path(os.getenv("SILVER_ROOT", r"C:\RawData\Eversys_Cleaned"))

PATHS = {
    "product":  SILVER_ROOT / "Product_History",
    "cleaning": SILVER_ROOT / "Cleaning_History",
    "rinse":    SILVER_ROOT / "Rinse_History",
    "alerts":   SILVER_ROOT / "Info_Message_History",
}

PRODUCT_KEY_MAP = {
    0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8,
    8: 9, 9: 10, 10: 11, 11: 12, 12: 13, 13: 14, 14: 15,
    15: 16, 16: 17, 17: 18, 18: 19, 19: 20, 20: 21, 255: 22,
}

HOPPER_KEY_MAP = {0: 1, 1: 2, 2: 3, 3: 4, 255: 5}
STOP_KEY_MAP = {0: 1, 1: 2, 2: 3, 3: 4}
POWDER_STATUS_MAP = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5}
TABS_STATUS_MAP = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8}
DETERGENT_STATUS_MAP = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8, 8: 9, 9: 10}
RINSE_TYPE_MAP = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 7: 8, 255: 9}
FLOW_STATUS_MAP = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7}
NOZZLE_STATUS_MAP = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5, 5: 6, 6: 7, 255: 8}

BATCH_SIZE = 5000

SILVER_METADATA_COLS = ["source_file", "file_timestamp", "ingestion_timestamp"]