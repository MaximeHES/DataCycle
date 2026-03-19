import logging
import sys
from datetime import datetime
from sqlalchemy import text
from connection import get_sqlalchemy_engine
from load_facts import (
    load_fact_production,
    load_fact_cleaning,
    load_fact_rinse,
    load_fact_alerts,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
    ]
)
logger = logging.getLogger(__name__)


def main():
    start = datetime.now()
    logger.info("=" * 60)
    logger.info("ETL START — Silver to Azure SQL")
    logger.info("=" * 60)

    engine = get_sqlalchemy_engine()

    # Test connection
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection OK")
    except Exception as e:
        logger.error(f"Database connection FAILED: {e}")
        sys.exit(1)

    # Load each fact table
    for loader, name in [
        (load_fact_production, "fact_production"),
        (load_fact_cleaning,   "fact_cleaning"),
        (load_fact_rinse,      "fact_rinse"),
        (load_fact_alerts,     "fact_alerts"),
    ]:
        try:
            loader(engine)
        except Exception as e:
            logger.error(f"{name} FAILED: {e}", exc_info=True)

    elapsed = datetime.now() - start
    logger.info("=" * 60)
    logger.info(f"ETL COMPLETE — Duration: {elapsed}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()