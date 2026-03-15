from sqlalchemy import create_engine, text
from etl.config import DATABASE_URL

engine = create_engine(DATABASE_URL)

with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS etl_run_log(
            run_id         SERIAL PRIMARY KEY,
            run_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            rows_extracted INTEGER,
            rows_cleaned   INTEGER,
            rows_inserted  INTEGER,
            rows_rejecteD  INTEGER,
            status         VARCHAR(20),
            notes          TEXT
        );
    """))

    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS data_quality_log (
            log_id            SERIAL PRIMARY KEY,
            run_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            total_rows        INTEGER,
            missing_order_id  INTEGER,
            duplicate_rows    INTEGER,
            invalid_dates     INTEGER,
            negative_values   INTEGER,
            rows_passed       INTEGER
        );
    """))

    conn.commit()
    print("✅ Tables created successfully in Railway!!")