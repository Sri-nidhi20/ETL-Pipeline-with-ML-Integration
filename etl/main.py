from sqlalchemy import create_engine
from etl.config import DATABASE_URL
from etl.extract import extract_data
from etl.validate import validate_data
from etl.transform import transform_data
from etl.incremental import get_existing_order_ids, filter_new_records
from etl.load import load_data

def run_pipeline(filepath):
    """
    Runs the full ETL pipeline on the given CSV file.
    Filepath = path to the CSV file.
               example: 'data/sample_data.csv'
               
    Returns a summary dictionary with all the results.
    """

    print("\n" + "="*50)
    print("  ETL PIPELINE STARTED")
    print("="*50 + "\n")
    #step 01 create databse engine (connection)
    print("[MAIN] Connecting to database...")
    engine = create_engine(DATABASE_URL)
    print("[MAIN] Database connection established!")

    #step 02 extract
    raw_df = extract_data(filepath)

    #step 03
    clean_df, rejected_df, report = validate_data(raw_df)

    #step 04 Transform
    transformed_df = transform_data(clean_df)

    #step 05 incremental check
    existing_ids = get_existing_order_ids(engine)
    new_records = filter_new_records(transformed_df, existing_ids)

    #step 06 load
    rows_inserted, status = load_data(new_records, report, engine)

    #step 07 build summary
    summary = {
        "status" : status,
        "rows_extracted" : report.get("total_rows_input", 0),
        "rows_passed" : report.get("rows_passed", 0),
        "rows_rejected" : report.get("rows_rejected", 0),
        "rows_inserted" : rows_inserted,
        "rejected_df" : rejected_df,
        "report" : report,
    }
    print("\n" + "="*50)
    print("  ETL PIPELINE COMPLETE")
    print(f"  Status          : {status}")
    print(f"  Rows Extracted  : {summary['rows_extracted']}")
    print(f"  Rows Passed     : {summary['rows_passed']}")
    print(f"  Rows Rejected   : {summary['rows_rejected']}")
    print(f"  Rows Inserted   : {summary['rows_inserted']}")
    print("="*50 + "\n")

    return summary

if __name__ == "__main__":
    summary = run_pipeline("data/sample_data.csv")