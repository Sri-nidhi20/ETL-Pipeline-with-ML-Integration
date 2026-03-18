#smart data profiling module
#automatically analyzes any uploaded csv and generates a comprehensive data quality and statistical report
import pandas as pd
import numpy as np

def generate_profile(df):
    """
    Takes a DataFrame and generates a complete profile report.
    Returns a dictionary with all profile information.
    """

    print("[PROFILER] Generating data profile...")

    profile = {}

    #basic info
    profile["total_rows"] = len(df)
    profile["total_columns"] = len(df.columns)
    profile["total_cells"] = len(df) * len(df.columns)
    profile["memory_usage"] = f"{df.memory_usage(deep=True).sum() / 1024:.2f} KB"

    #missing values
    missing = df.isnull().sum()
    missing_percent = (missing / len(df) * 100).round(2)

    profile["missing_values"] = pd.DataFrame({
        "Column" : missing.index,
        "Missing Count" : missing.values,
        "Missing %" : missing_percent.values,
        "Status" : [
            "✅ Complete" if x == 0
            else "⚠️ Partial" if x < len(df) * 0.5
            else "❌ Critical"
            for x in missing.values
        ]
    })

    #duplicates
    dup_mask = df.duplicated()
    dup_count = int(dup_mask.sum())
    profile["duplicate_rows"] = dup_count
    profile["duplicate_percent"] = round(
        dup_count / len(df) * 100, 2
    ) if len(df) > 0 else 0.0

    # data types
    profile["dtypes"] = pd.DataFrame({
        "Column" : df.dtypes.index,
        "Data Type" : df.dtypes.values.astype(str),
        "Category" : [
            "Numeric" if str(t) in ["int64", "float64"]
            else "Date" if "datetime" in str(t)
            else "Text"
            for t in df.dtypes.values
        ]
    })

    #numeric column stats
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

    if numeric_cols:
        stats = df[numeric_cols].describe().T
        stats["missing"] = df[numeric_cols].isnull().sum()
        stats["missing_%"] = (
            df[numeric_cols].isnull().sum() / len(df) * 100
        ).round(2)
        profile["numeric_stats"] = stats
        profile["numeric_cols"] = numeric_cols
    else:
        profile["numeric_stats"] = pd.DataFrame()
        profile["numeric_cols"] = []

    #text column stats
    text_cols = df.select_dtypes(include=["object"]).columns.tolist()

    if text_cols:
        text_stats = []
        for col in text_cols:
            text_stats.append({
                "Column" : col,
                "Unique Values" : df[col].nunique(),
                "Most Common" : df[col].mode()[0] if not df[col].mode().empty else "N/A",

            })
        profile["text_stats"] = pd.DataFrame(text_stats)
        profile["text_cols"] = text_cols
    else:
        profile["text_stats"] = pd.DataFrame()
        profile["text_cols"] = []

    #Data Quality Score
    #missing values score
    total_missing = missing.sum()
    missing_score = max(0, 40 - (total_missing / profile["total_cells"] * 100))

    #duplicate score
    duplicate_score = max(0, 30 - float(profile["duplicate_percent"]))

    #completeness score
    complete_cols = sum(1 for x in missing.values if x == 0)
    completeness = complete_cols / len(df.columns) * 30

    profile["quality_score"] = round(
        missing_score + duplicate_score + completeness, 1
    )

    #quality grade
    score = profile["quality_score"]
    if score >= 90:
        profile["quality_grade"] = "A - Excellent 🟢"
    elif score >= 75:
        profile["quality_grade"] = "B - Good 🟡"
    elif score >= 60:
        profile["quality_grade"] = "C - Average 🟠"
    else:
        profile["quality_grade"] = "D - Poor 🔴"
    
    print(f"[PROFILER] Profile Complete - Quality Score: {profile['quality_score']} / 100")

    return profile 