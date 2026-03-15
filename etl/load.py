import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from etl.config import DATABASE_URL

def load_data(new_records, report, engine):
    """
    Inserts new clean records into the sales_clean table.
    Logs the ETL run into etl_run_log.
    Logs data quality details into data_quality_log.
    
    new_records = DataFrame of only new rows (from incremental.py)
    report = quality report dictionary (from validate.py)
    engine = SQLALchemy database engine (connection)
    """
    rows_inserted = 0
    status = "SUCCESS"
    notes = ""
    
    try:
        if len(new_records) > 0:
            print(f"[LOAD] Inserting {len(new_records)} new records into sales_clean...")
            new_records.to_sql(
                name = "sales_clean",
                con = engine,
                if_exists = "append",
                index = False
            )
            rows_inserted = len(new_records)
            print(f"[LOAD] Successfully inserted {rows_inserted} rows!")
        else:
            print("[LOAD] No new records to insert. Database is already up to date.")

    except Exception as e:
        status = "FAILED"
        notes = str(e)
        print(f"[LOAD] ERROR during insert: {e}")

    #log the ETL run into etl_run_log table 
    #this records every run - success or failure
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                              INSERT INTO etl_run_log
                                   (run_timestamp, rows_extracted, rows_cleaned,
                                   rows_inserted, rows_rejected, status, notes)
                              VALUES
                                   (:timestamp, :extracted, :cleaned,
                                    :inserted, :rejected, :status, :notes)
            """), {
                "timestamp" : datetime.now(),
                "extracted" : report.get("total_rows_input", 0),
                "cleaned" : report.get("rows_passed", 0),
                "inserted" : rows_inserted,
                "rejected" : report.get("rows_rejected", 0),
                "status" : status,
                "notes" : notes
            })
            #commit() saves the log entry permanently
            conn.commit()
            print("[LOAD] ETL run logged successfully.")
    except Exception as e:
        print(f"[LOAD] ERROR writing to etl_run_log: {e}")
    #log data quality details into data_quality_log
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO data_quality_log
                    (run_timestamp, total_rows, missing_order_id,
                     duplicate_rows, invalid_dates, negative_values, rows_passed)
                VALUES
                    (:timestamp, :total, :missing_oid,
                    :dupes, :bad_dates, :negatives, :passed)
            """), {
                "timestamp" : datetime.now(),
                "total" : report.get("total_rows_input", 0),
                "missing_oid" : 0,
                "dupes" : report.get("duplicates_removed", 0),
                "bad_dates" : 0,
                "negatives" : report.get("negatives_values", 0),
                "passed" : report.get("rows_passed", 0)
            })
            conn.commit()
            print("[LOAD] Data quality log saved.")
    except Exception as e:
        print(f"[LOAD] ERROR writing to data_quality_log: {e}")
    return rows_inserted, status