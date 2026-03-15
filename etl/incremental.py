import pandas as pd
from sqlalchemy import create_engine, text
from etl.config import DATABASE_URL

def get_existing_order_ids(engine):
    """
    Connects to PostgreSQL and fetches all order_ids
    that are already stored in the sales_clean table.
    
    Returns a Python SET of order_id strings.
    A set is like a list but checks membership much faster.
    """
    print ("[INCREMENTAL] Fetching existing order IDs from database...")

    #handling errors
    try:
        #engine.connect() as conn opens a database connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT order_id FROM sales_clean"))
            rows = result.fetchall()
            existing_ids = set(row[0] for row in rows)

            print(f"[INCREMENTAL] Found {len(existing_ids)} existing records in DB")
            return existing_ids
    except Exception as e:
        print(f"[INCREMENTAL] Could not fetch existing IDs: {e}")
        return set()

def filter_new_records(df, existing_ids):
    """
    Compares the cleaned DataFrame against existing order_ids.
    Keeps only rows that are NEW (not already in the database).
    
    df = the cleaned DataFrame from transform.py 
    existing_ids = the set of order_ids already in the database
    """
    print("[INCREMENTAL] Filtering for new records only...")

    total_rows = len(df)
    
    new_records =df[~df["order_id"].isin(existing_ids)]
    #new vs old
    already_exists = total_rows - len(new_records)

    print(f"[INCREMENTAL] Total rows in file: {total_rows}")
    print(f"[INCREMENTAL] Already in database: {already_exists}")
    print(f"[INCREMENTAL] New records to insert: {len(new_records)}")
    return new_records