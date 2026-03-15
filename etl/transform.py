import pandas as pd
def transform_data(df):
    """
    cleans and formats the validated dataframe.
    what it does:
    - converts order_date to proper date format
    - fills any remaining missing numeric values with 0 
    - strips extra spaces from text columns 
    - makes sure quantity is a whole number (integer)
    - rounds price and amount columns to 2 decimal places
    Returns the fully cleaned dataframe.
    """
    print("[TRANSFORM] Starting data transformation...")
    df = df.copy()
    
    #fix 01 coonvert order_date to proper date format
    df["order_date"] = pd.to_datetime(df["order_date"], errors = "coerce")
    #drop any rows thats couldn't be converted
    before = len(df)
    df = df.dropna(subset = ["order_date"])
    after = len(df)
    if before != after:
        print(f"[TRANSFORM] Dropped {before - after} rows with invaid dates")

    #fix 02 strip extra spaces from all text columns
    text_columns = [
        "order_id", "customer_id", "product_id",
        "product_name", "category", "region",
        "payment_method", "status"
    ]

    for col in text_columns:
        df[col] = df[col].astype(str).str.strip()
    
    #fix 03 fill missing numeric values with 0
    df["quantity"] = df["quantity"].fillna(0)
    df["unit_price"] = df["unit_price"].fillna(0)
    df["total_amount"] = df["total_amount"].fillna(0)

    #fix 04 make sure quantity is a whole number
    df["quantity"] = df["quantity"].astype(int)

    #fix 05 round prices to 2 decimal places
    df["unit_price"] = df["unit_price"].round(2)
    df["total_amount"] = df["total_amount"].round(2)

    #fix 6 standardize text casing
    df["region"] = df["region"].str.title()
    df["category"] = df["category"].str.title()
    df["payment_method"] = df["payment_method"].str.title()
    df["status"] = df["status"].str.title()
    
    print(f"[TRANSFORM] Transformation complete. Rows ready: {len(df)}")
    return df