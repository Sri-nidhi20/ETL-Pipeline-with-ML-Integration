# Sales Prediction using Linear Regression
#what it does: 
#looks at past monthly sales and predicts
#what the next few months' sales will be

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score

def run_prediction(df):
    """
    Takes the cleaned sales DataFrame and predicts
    future monthly sales using Linear Regression.
    
    Returns:
    - monthly_df  : actual monthly sales history
    - forecast_df : predicted future months
    - metrics     : model performance (R2, RMSE)
    - model       : trained regression model
    """

    print("[ML] Starting sales prediction...")

    df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"])

    #create year-month column string column like '2024-01'
    df["month_str"] = df["order_date"].dt.strftime("%Y-%m")

    #Agrregate monthly sales
    monthly_agg = df.groupby("month_str").agg(
        total_sales = ("total_amount", "sum"),
        total_orders = ("order_id", "count")
    ).reset_index()

    #sort by month string 
    monthly_agg = monthly_agg.sort_values("month_str").reset_index(drop = True)

    monthly_agg["month_num"] = range(1, len(monthly_agg) + 1)

    monthly_agg["type"] = "Actual"

    print(f"[ML] {len(monthly_agg)} months of history found")
    print(f"[ML] Columns: {monthly_agg.columns.tolist()}")

    #Train Model
    x = monthly_agg[["month_num"]]
    y = monthly_agg["total_sales"]

    model = LinearRegression()
    model.fit(x,y)

    #evaluate
    y_pred = model.predict(x)
    r2 = r2_score(y, y_pred)
    rmse = np.sqrt(mean_squared_error(y, y_pred))

    metrics = {
        "R2 Score" : round(r2, 3),
        "RMSE" : round(rmse, 2),
    }

    # Predict Next 3 Months
    last_month_num = int(monthly_agg["month_num"].max())
    last_month_str = monthly_agg["month_str"].max()

    #calculate next 3 months
    last_date =pd.to_datetime(last_month_str + "-01")

    future_rows = []
    for i in range(1, 4):
        future_month_num = last_month_num + i
        future_date = last_date + pd.DateOffset(months = i)
        future_month_str = future_date.strftime("%Y-%m")
        predicted_sales = model.predict([[future_month_num]])[0]

        future_rows.append({
            "month_str" : future_month_str,
            "month_num" : future_month_num,
            "total_sales" : round(max(predicted_sales, 0), 2),
            "type" : "Forecast"
        })

    forecast_df = pd.DataFrame(future_rows)

    print(f"[ML] Forecast complete for next 3 months!")

    return monthly_agg, forecast_df, metrics, model
