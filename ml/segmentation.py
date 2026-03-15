# CUSTOMER SEGMENTATION USING KMEANS CLUSTERING
#Groups Customers into segments based on their buying behavious
#this is called RFM Analysis:
# r = Recency -> how recently did they buy?
# f = frequency -> how many times did they buy?
# m = monetary -> how much did they spend total?
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

def run_segmentation(df, n_clusters = 4):
    """
    Takes the cleaned sales DataFrame and groups customers
    into segments based on their buying behaviour.
    
    Returns:
    - rfm_df : DataFrame with each customer and their segment
    - model : the trained KMeans model
    - scaler : the scaler used (needed for future predictions)
    """

    print("[ML] Staring customer segmentation...")

    #step 01: build RFM Table
    #we need to calculate 3 things per customer

    #convert order_date to datetime if not already
    df["order_date"] = pd.to_datetime(df["order_date"])

    #reference date = the most recent data in the dataset + 1 day
    #we use this to calculate "how many days since last purchase
    reference_date = df["order_date"].max() + pd.Timedelta(days=1)

    rfm_df = df.groupby("customer_id").agg(
        Recency = ("order_date", lambda x: (reference_date - x.max()).days),
        Frequency = ("order_id", "count"),
        Monetary = ("total_amount", "sum")
    ).reset_index()

    print(f"[ML] RFM table built for {len(rfm_df)} customers")

    # step 02: Scale the Features
    #KMeans works with distances, so all values must be
    #on the same scale. without scaling, Monetary (5000/-)
    #would dominate over Frequency (3 orders).
    scaler = StandardScaler()
    rfm_scaled = scaler.fit_transform(rfm_df[["Recency", "Frequency", "Monetary"]])

    # Step 03 Train KMeans Model --
    #n_clusters = how many groups to create
    # random_state = 42 means results are reproducible
    model = KMeans(n_clusters = n_clusters, random_state = 42, n_init = 10)
    rfm_df["Cluster"] = model.fit_predict(rfm_scaled)

    #step 04: Label Each Cluster --
    # After clustering, we figure out what each cluster means
    cluster_summary = rfm_df.groupby("Cluster").agg(
        Avg_Recency = ("Recency", "mean"),
        Avg_Frequency = ("Frequency", "mean"),
        Avg_Monetary = ("Monetary", "mean"),
        Customer_Count = ("customer_id", "count")
    ).reset_index()

    #sort by Monetary descending to rank clusters
    cluster_summary = cluster_summary.sort_values("Avg_Monetary", ascending = False)
    cluster_summary["Rank"] = range(len(cluster_summary))

    #assign human-readable labels based on ranl
    labels = {0: "💎 High Value", 1: "🟢 Loyal", 2: "🟡 At Risk", 3: "🔴 Lost"}
    cluster_summary["Segment"] = cluster_summary["Rank"].map(labels)

    #Map labels back to rfm_df
    cluster_to_segment = dict(
        zip(cluster_summary["Cluster"], cluster_summary["Segment"])
    )
    rfm_df["Segment"] = rfm_df["Cluster"].map(cluster_to_segment)

    print(f"[ML] Segmentation complete! Segments: {rfm_df['Segment'].value_counts().to_dict()}")
    
    return rfm_df, cluster_summary, model, scaler