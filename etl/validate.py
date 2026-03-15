import pandas as pd
def validate_data(df) :
    """
    checks the dataframe for data quality issues.
    returns:
    -cleaned_df : rows that passed all checks
    -rejected_df : rows that failed(with a reason column added)
    - report : a dictionary with counts of each problem found
    """
    print("[VALIDATE] Starting data quality checks...")

    rejected_rows = []

    df = df.copy()

    #check 01 missing order is
    missing_order_id = df[df["order_id"].isnull()]
    if not missing_order_id.empty:
        missing_order_id = missing_order_id.copy()
        missing_order_id["rejection_reason"] = "Missing order_id"
        rejected_rows.append(missing_order_id)
    
    df = df[df["order_id"].notnull()] 

    #check 02 missing customer id
    missing_customer = df[df["customer_id"].isnull()]
    if not missing_customer.empty:
        missing_customer = missing_customer.copy()
        missing_customer["rejection_reason"] = "Missing customer_id"
        rejected_rows.append(missing_customer)
    
    df = df[df["customer_id"].notnull()]

    #check 03 missing order date
    missing_date = df[df["order_date"].isnull()]
    if not missing_date.empty:
        missing_date = missing_date.copy()
        missing_date["rejection_reason"] = "Missing order_date"
        rejected_rows.append(missing_date)

    df = df[df["order_date"].notnull()]

    #check 04 negative total_amount
    negative_values = df[df["total_amount"] < 0]
    if not negative_values.empty:
        negative_values = negative_values.copy()
        negative_values["rejection_reason"] = "Negative total_amount"
        rejected_rows.append(negative_values)
    
    df = df[df["total_amount"] >= 0]

    #check 05 duplicate order_id
    duplicate_count = df.duplicated(subset=["order_id"]).sum()
    duplicate_rows = df[df.duplicated(subset = ["order_id"], keep = "first")].copy()
    if not duplicate_rows.empty:
        duplicate_rows["rejection_reason"] = "Duplicate order_id"
        rejected_rows.append(duplicate_rows)
    
    df = df.drop_duplicates(subset=["order_id"], keep = "first")

    #build the rejected dataframe
    if rejected_rows:
        rejected_df = pd.concat(rejected_rows, ignore_index = True)
    else:
        rejected_df = pd.DataFrame() 
    
    # build the quality report
    report = {
        "total_rows_input" : len(df) + len(rejected_df),
        "missing_customer_id" : len(missing_customer),
        "missing_order_date" : len(missing_date),
        "negative_values" : len(negative_values),
        "duplicates_removed" : int(duplicate_count),
        "rows_passed" : len(df),
        "rows_rejected" : len(rejected_df),
    }
    print(f"[VALIDATE] Rows passed : {report['rows_passed']}")
    print(f"[VALIDATE] Rows rejected : {report['rows_rejected']}")
    return df, rejected_df, report
