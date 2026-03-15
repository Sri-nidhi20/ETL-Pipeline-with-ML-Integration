# Anomaly Detection using Isolation Forest
#what it does:
# finds suspicious or unusual orders in the data

import pandas as pd
from sklearn.ensemble import IsolationForest

def run_anomaly_detection(df, contamination = 0.05):
    """
    Scans the sales data for unusual/suspicious orders.
    
    contamination = what % of data we expect to be anomalies
                    0.05 means we expect 5% of orders are suspicious
    
    Returns:
    - result_df : full DataFrame with anomaly labels
    - anomaly_df : only the susicious rows
    - summary : count of normal vs anomalous orders
    """

    print("[ML] Starting anomaly detection...")

    df = df.copy()

    #step 01: select features for detection --
    #we look at quantity, unit_price, total_amount these are the most likely columns to have suspicious values
    features = ["quantity", "unit_price", "total_amount"]

    #drop rows where these columns have nulls
    df_clean = df.dropna(subset=features).copy()

    x = df_clean[features]

    #step 02: Train Isolation Forest --
    #Isolation Forest works by randomly isolating observations
    #anomalies areeasier to isolate = shorter path in the tree
    model = IsolationForest(
        contamination = contamination,
        random_state = 42
    )

    #predict() returns 1 = normal order, -1 = anomaly/suspicious order
    df_clean["anomaly_score"] = model.fit_predict(x)

    df_clean["anomaly_label"] = df_clean["anomaly_score"].map({
        1: "✅ Normal",
        -1: "🚨 Suspicious"
    })

    # step 03: extract anomalies
    anomaly_df = df_clean[df_clean["anomaly_score"] == -1].copy()
    normal_df = df_clean[df_clean["anomaly_score"] == 1].copy()

    summary = {
        "total_orders" : len(df_clean),
        "normal_orders" : len(normal_df),
        "suspicious_orders" : len(anomaly_df),
    }

    print(f"[ML] Anomaly detection complete!")
    print(f"[ML] Normal: {summary['normal_orders']} | Suspicious: {summary['suspicious_orders']}")

    return df_clean, anomaly_df, summary 