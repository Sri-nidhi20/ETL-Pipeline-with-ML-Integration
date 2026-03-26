import streamlit as st
import tempfile
import os
import pandas as pd
import plotly.express as px
import sys
import os
from sqlalchemy import create_engine

#add the project root to python path so we can import etl modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.main import run_pipeline
from etl.config import DATABASE_URL
from etl.profiler import generate_profile 

from ml.segmentation import run_segmentation
from ml.prediction import run_prediction
from ml.anomaly import run_anomaly_detection 

#PAGE CONFIGURATION
st.set_page_config(
    page_title = "ETL Pipeline Dashboard",
    page_icon = "🔄️",
    layout = "wide"
)

#custom styling
st.markdown("""
    <style>
        .main { background-color: #f8f9fa; }
        .metric-card {
            background: white;
            padding: 20px;
            border-radius: 10px;
            border-left: 5px solid #4CAF50;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        .rejected-card {
            border-left: 5px solid #f44336;
        }
        .stButton > button {
            background-color: #4CAF50;
            color: white;
            font-size: 18px;
            padding: 10px 30px;
            border-radius: 8px;
            border: none;
            width: 100%;
        }
        .stButton > button:hover {
            background-color: #45a049;
        }
    </style>
""", unsafe_allow_html = True)

#Sidebar Navigation
st.sidebar.image("https://img.icons8.com/color/96/data-configuration.png", width = 80)
st.sidebar.title("ETL Pipeline")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    ["🏠 Home & Upload",
     "📊 Data Quality Report",
     "🗃️ View Clean Data",
     "📋 ETL Run Logs",
     "🤖 ML Models",
     "🧠 Ask DALE"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Project:** BCA Final Year")
st.sidebar.markdown("**Tech Stack:** Python, PostgreSQL, Streamlit")

#Database Engine
@st.cache_resource
def get_engine():
    """
    Creates and caches the database connection.
    @st.cache_resource means it only connects once
    and reuses the connection for speed.
    """
    return create_engine(DATABASE_URL)

engine = get_engine()

#Page 01: Home & Upload
if page == "🏠 Home & Upload":

    st.title("🔃 Automated ETL Pipeline")
    st.markdown("### Upload any e-commerce CSV and run the pipeline")
    st.markdown("---")

    #--Required VS Optional Columns
    required_columns = {
        "order_id" : "Unique Order ID",
        "customer_id" : "Customer ID",
        "order_date" : "Order Date",
        "quantity" : "Quantity",
        "unit_price" : "Unit Price",
        "total_amount" : "Total Amount",
    }

    optional_columns = {
        "product_id" : ("Product ID", "UNKNOWN"),
        "product_name" : ("Product Name", "Unknown Product"),
        "category" : ("Category", "Uncategorized"),
        "region" : ("Region", "Unknown"),
        "payment_method" : ("Payment Method", "Unknown"),
        "status" : ("Order Status", "Delivered"),
    }

    #-- file upload --
    st.markdown("### 📤 Step 01: Upload Your Dataset")
    st.markdown("Upload **any** e-commerce CSV")

    uploaded_file = st.file_uploader(
        "Choose a CSV file",
        type = ["csv"],
        help = "Upload Your e-commerce sales CSV file"
    )

    if uploaded_file is not None:
        user_df = pd.read_csv(uploaded_file)

        st.success(f"✅ File Uploaded: **{uploaded_file.name}**")
        st.markdown(f"**{len(user_df)} rows** and **{len(user_df.columns)} columns** detected")
        st.markdown("#### 👀 Preview of Your File")
        st.dataframe(user_df.head(5), use_container_width = True)

        st.markdown("---")

        #auto data profiling
        st.markdown("### 🔍 Instant Data Profile")

        with st.spinner("🔍 Analyzing your data quality ..."):
            profile = generate_profile(user_df)
        
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                "🏆 Quality Score",
                f"{profile['quality_score']} / 100"
            )
        with col2:
            st.metric(
                "📊 Grade",
                profile["quality_grade"]
            )
        with col3:
            st.metric(
                "❓ Duplicate Rows",
                profile["duplicate_rows"]
            )
        with col4:
            missing_total = profile["missing_values"]["Missing Count"].sum()
            st.metric(
                "⚠️ Missing Values",
                int(missing_total)
            )
        
        with st.expander("📋 View Detailed Data Profile"):
            st.markdown("#### ❓ Missing Values by Column")
            st.dataframe(
                profile["missing_values"],
                use_container_width = True
            )

            if len(profile["numeric_cols"]) > 0:
                st.markdown("##### 🔢 Numeric Column Statistics")
                st.dataframe(
                    profile["numeric_stats"].round(2),
                    use_container_width = True
                )
            
            if len(profile["numeric_cols"]) > 0:
                st.markdown("##### 📈 Value Distributions")
                cols_per_row = 3
                numeric_cols = profile["numeric_cols"]

                for i in range(0, len(numeric_cols), cols_per_row):
                    cols = st.columns(cols_per_row)
                    for j, col_name in enumerate(
                        numeric_cols[i:i+cols_per_row]
                    ):
                        with cols[j]:
                            fig_hist = px.histogram(
                                user_df,
                                x = col_name,
                                title = f"{col_name}",
                                color_discrete_sequence = ["#4CAF50"]
                            )
                            fig_hist.update_layout(
                                height = 250,
                                showlegend = False,
                                margin = dict(t = 30, b=0, l=0, r=0)
                            )
                            st.plotly_chart(
                                fig_hist,
                                use_container_width = True
                            )

            if len(profile["numeric_cols"]) > 1:
                st.markdown("##### 🔥 Correlation Heatmap")
                corr_matrix = user_df[
                    profile["numeric_cols"]
                ].corr().round(2)

                fig_corr = px.imshow(
                    corr_matrix,
                    color_continuous_scale = "RdBu",
                    aspect = "auto"
                )
                st.plotly_chart(fig_corr, use_container_width = True)

        #store profile in session state for other pages
        st.session_state["profile"] = profile
        st.session_state["user_df"] = user_df


        #--COLUMN MAPPING
        user_columns = user_df.columns.tolist()
        options = ["-- Skip --"] + user_columns

        st.markdown("#### 🔀 Step 02: Map Your Columns")
        st.markdown("""
        - 🔴 **Required columns** - must be mapped for pipeline to run
        - 🟡 **Optional columns** - if not available, system fills a default value
        """)

        column_mapping = {}

        #--REQUIRED COLUMNS --
        st.markdown("#### 🔴 Required Columns")
        col1, col2 = st.columns(2)
        req_list = list(required_columns.items())

        for i, (system_col, label) in enumerate(req_list):
            with col1 if i % 2 == 0 else col2:
                #try to auto-detect matching column
                default_index = 0
                for j, user_col in enumerate(user_columns):
                    if user_col.lower().strip().replace(" ", "_") == system_col.lower():
                        default_index = j+1
                        break
                
                selected = st.selectbox(
                    label = f"**{label}** -> `{system_col}`",
                    options = options,
                    index = default_index,
                    key = f"req_{system_col}"
                )
                column_mapping[system_col] = selected
        #--OPTIONAL COLUMNS --
        st.markdown("#### 🟡 Optional Columns")
        col3, col4 = st.columns(2)
        opt_list = list(optional_columns.items())

        for i, (system_col, (label, default_val)) in enumerate(opt_list):
            with col3 if i % 2 == 0 else col4:
                #try to auto-detect matching column
                default_index = 0
                for j, user_col in enumerate(user_columns):
                    if user_col.lower().strip().replace(" ", "_") == system_col.lower():
                        default_index = j+1
                        break
                selected = st.selectbox(
                    label = f"{label} -> `{system_col}` *(default: '{default_val}')*",
                    options = options,
                    index = default_index,
                    key = f"opt_{system_col}"
                )
                column_mapping[system_col] = selected
        st.markdown("---")

        #--VALIDATE REQUIRED COLUMNS --
        unmapped_required = [
            required_columns[col]
            for col in required_columns
            if column_mapping.get(col) == "-- Skip --"
        ]

        if unmapped_required:
            st.error(
                f"❌ These **required** columns are not mapped: "
                f"**{', '.join(unmapped_required)}**. "
                f"Pipeline cannot run without them."
            )
        else:
            st.success("✅ All required columns mapped! Ready to run.")

            #show mapping summary
            with st.expander("📋 View Column Mapping Summary"):
                summary_rows = []
                for sys_col, user_col in column_mapping.items():
                    if sys_col in optional_columns:
                        default_val = optional_columns[sys_col][1]
                        status = (
                            f"<- from your column '{user_col}'"
                            if user_col != "-- Skip --"
                            else f"<- will use default: '{default_val}'"
                        )
                    else:
                        status = f"<- from your column '{user_col}'"
                    
                    summary_rows.append({
                        "System Column" : sys_col,
                        "Your Column" : user_col,
                        "Status" : status
                    })
                st.dataframe(
                    pd.DataFrame(summary_rows),
                    use_container_width = True
                )
            st.markdown("#### ▶️ Step 03: Run The ETL Pipeline")

            if st.button("🚀 Run ETL Pipeline"):
                #apply mapping
                mapped_df = user_df.copy()
                #rename mapped columns to system names
                rename_map = {
                    v: k for k, v in column_mapping.items()
                    if v != "-- Skip --"
                }
                mapped_df = mapped_df.rename(columns=rename_map)

                #fill missing optional columns with defaults
                for sys_col, (label, default_val) in optional_columns.items():
                    if sys_col not in mapped_df.columns:
                        mapped_df[sys_col] = default_val
                
                #keep only system columns
                final_df = mapped_df[list({**required_columns, **optional_columns}.keys())]

                #save to temp csv
                temp_dir = tempfile.gettempdir()
                temp_path = os.path.join(temp_dir, "uploaded_data.csv")
                final_df.to_csv(temp_path, index = False)

                #Run Pipeline
                with st.spinner("⏳ Pipeline is running... please wait"):
                    try:
                        summary = run_pipeline(temp_path)
                        st.session_state["summary"] = summary
                        st.session_state["pipeline_ran"] = True
                    except Exception as e:
                        st.error(f"❌ Pipeline failed: {e}")
                        st.stop()
                        
                #show results
                st.success("✅ Pipeline completed successfully!")
                st.markdown("### 📊 Pipeline Results")

                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    st.metric("📥 Rows Extracted", summary["rows_extracted"])
                with c2:
                    st.metric("✅ Rows Passed", summary["rows_passed"])
                with c3:
                    st.metric("❌ Rows Rejected", summary["rows_rejected"])
                with c4:
                    st.metric("💾 Rows Inserted", summary["rows_inserted"])
                
                if summary["rows_rejected"] > 0:
                    st.markdown("---")
                    st.markdown("### ❌ Rejected Rows")
                    st.warning("These rows were removed due to data quality issues: ")
                    st.dataframe(summary["rejected_df"], use_container_width = True)


#page 02: Data Quality Report
elif page == "📊 Data Quality Report":
    st.title("📊 Data Quality Report")
    st.markdown("---")
    if "summary" not in st.session_state:
        st.info("❗No Pipeline has been run yet. Go to **Home & Upload** to run the pipeline first.")
    else:
        summary = st.session_state["summary"]
        report = summary["report"]
        
        st.success("Showing results from the last pipeline run.")

        total_rows = int(report.get("total_rows_input", 0))
        rows_passed = int(report.get("rows_passed", 0))
        rows_rejected = int(report.get("rows_rejected", 0))
        missing_cust = int(report.get("missing_customer_id", 0))
        missing_date = int(report.get("missing_order_date", 0))
        neg_values = int(report.get("negative_values", 0))
        duplicates = int(report.get("duplicates_removed", 0))

        #quality metrics
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### 📋 Quality Summary")
            quality_data = pd.DataFrame({
                "Metric" : ["Total Rows Input", "Rows Passed", "Rows Rejected",
                            "Missing Customer ID", "Missinhg Order Date",
                            "Negative Values", "Duplicates Removed"],
                "Count" : [
                    total_rows,
                    rows_passed,
                    rows_rejected,
                    missing_cust,
                    missing_date,
                    neg_values,
                    duplicates
                ]
            })
            st.dataframe(pd.DataFrame(quality_data), use_container_width = True)
        with col2:
            st.markdown("### 🥧 Pass VS Reject")
            
            pie_data = pd.DataFrame({
                "Category" : ["Passed", "Rejected"],
                "Count" : [rows_passed, rows_rejected]
            })

            fig = px.pie(
                pie_data,
                names = "Category",
                values = "Count",
                color_discrete_sequence = ["#4CAF50", "#f44336"]
            )
            st.plotly_chart(fig, use_container_width = True)
        
        #issues breakdown bar chart
        st.markdown("### 📊 Issues Breakdown")
        
        issues_data = pd.DataFrame({
            "Issue" : ["Missing Customer ID", "Missing Order Date",
                       "Negative Values", "Duplicates"],
            "Count" : [
                missing_cust,
                missing_date,
                neg_values,
                duplicates
            ]
        })
        fig2 = px.bar(
            data_frame = issues_data,
            x = "Issue",
            y = "Count",
            color = "Issue",
            color_discrete_sequence = ["#FF6B6B", "#FFA500", "#FFD700", "#87CEEB"]
        )
        st.plotly_chart(fig2, use_container_width = True)

#Page 03 View Clean Data
elif page == "🗃️ View Clean Data":
    st.title("🗃️ Clean Data in Database")
    st.markdown("---")
    try:
        #read all clean data from postgresql
        df = pd.read_sql("SELECT * FROM sales_clean", engine)
        if df.empty:
            st.info("No data in database yet. Run the pipeline first.")
        else:
            st.success(f"✅ **{len(df)} total records** in database")

            #filters
            st.markdown("### 🔍 Filter Data")
            col1, col2, col3 = st.columns(3)

            with col1:
                categories = ["All"] + sorted(df["category"].unique().tolist())
                selected_cat = st.selectbox("Category", categories)
            
            with col2:
                regions = ["All"] + sorted(df["region"].unique().tolist())
                selected_region = st.selectbox("Region", regions)

            with col3:
                statuses = ["All"] + sorted(df["status"].unique().tolist())
                selected_status = st.selectbox("Status", statuses)
            
            #apply filters
            filtered_df = df.copy()
            if selected_cat != "All":
                filtered_df = filtered_df[filtered_df["category"] == selected_cat]
            if selected_region != "All":
                filtered_df = filtered_df[filtered_df["region"] == selected_region]
            if selected_status != "All":
                filtered_df = filtered_df[filtered_df["status"] == selected_status]
            
            st.markdown(f"Showing **{len(filtered_df)}** records")
            st.dataframe(filtered_df, use_container_width = True)

            #download clean data
            csv = filtered_df.to_csv(index = False)
            st.download_button(
                label = "⬇️ Download Filtered Data as CSV",
                data = csv,
                file_name = "clean_data.csv",
                mime = "text/csv"
            )
    except Exception as e:
        st.error(f"Could not load data: {e}")

#Page 04: ETL RUN LOGS
elif page == "📋 ETL Run Logs":
    st.title("📋 ETL Run History")
    st.markdown("---")

    try:
        logs_df = pd.read_sql(
            "SELECT * FROM etl_run_log ORDER BY run_timestamp DESC",
            engine
        )

        if logs_df.empty:
            st.info("No pipeline runs recorded yet.")
        else:
            st.success(f"✅ **{len(logs_df)} total pipeline runs** recorded")

            #summary metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Runs", len(logs_df))
            with col2:
                success_count = len(logs_df[logs_df["status"] == "SUCCESS"])
                st.metric("Successful Runs", success_count)
            with col3:
                total_inserted = logs_df["rows_inserted"].sum()
                st.metric("Total Rows Ever Inserted", int(total_inserted))
            
            st.markdown("---")
            st.markdown("### 📜 Full Run History")
            st.dataframe(logs_df, use_container_width = True)
    except Exception as e:
        st.error(f"Could not load logs: {e}")

#Page 05: ML Models
elif page == "🤖 ML Models":
    st.title("🤖 ML Analytics")
    st.markdown("### Run ML models on your cleaned sales data")
    st.markdown("---")

    #Load Data From DataBase
    try:
        df = pd.read_sql("SELECT * FROM sales_clean", engine)
    except Exception as e:
        st.error(f"Could not load data from database: {e}")
        st.stop()
    
    if df.empty:
        st.warning("⚠️ No data in database yet. Please run the ETL pipeline first.")
        st.stop()
    
    st.success(f"✅ **{len(df)} records** loaded from database for analysis")
    st.markdown("---")

    #model 01: customer segmentation
    st.markdown("## 👥 Model 01: Customer Segmentation")
    st.markdown("""
    Groups customers into segments based on:
    - **Recency** - How recently did they buy?
    - **Frequency** - How many times did they buy?
    - **Monetary** - How much did they spend?
    """)

    n_clusters = st.slider(
        "Number of customer segments",
        min_value = 2,
        max_value = 6,
        value = 4,
        help = "Choose how many groups to divide customers into"
    )

    if st.button("🚀 Run Customer Segmentation"):
        with st.spinner("Running segmentation..."):
            try:
                rfm_df, cluster_summary, model, scaler = run_segmentation(df, n_clusters)

                st.success("✅ Segmentation complete!")

                #metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Customers", len(rfm_df))
                with col2:
                    st.metric("Segments Created", n_clusters)
                with col3:
                    top_segment = rfm_df["Segment"].value_counts().index[0]
                    st.metric("Largest Segment", top_segment)

                st.markdown("---")

                #PIE CHART: Customers per segment
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown("### 🥧 Customers per Segment")
                    seg_counts = rfm_df["Segment"].value_counts().reset_index()
                    seg_counts.columns = ["Segment", "Count"]

                    fig1 = px.pie(
                        seg_counts,
                        names = "Segment",
                        values = "Count",
                        color_discrete_sequence = px.colors.qualitative.Set2
                    )
                    st.plotly_chart(fig1, use_container_width = True)

                # BAR CHART: Avg Spend per Segment --
                with col2:
                    st.markdown("### 💰 Avg Spending per Segment")
                    fig2 = px.bar(
                        cluster_summary,
                        x = "Segment",
                        y = "Avg_Monetary",
                        color = "Segment",
                        color_discrete_sequence = px.colors.qualitative.Set2
                    )
                    st.plotly_chart(fig2, use_container_width = True)
                
                # Scatter Plot: Frequency VS Monetary --
                st.markdown("### 📊 Frequency VS Spending by Segment")
                fig3 = px.scatter(
                    rfm_df,
                    x = "Frequency",
                    y = "Monetary",
                    color = "Segment",
                    size = "Monetary",
                    hover_data = ["customer_id", "Recency"],
                    color_discrete_sequence = px.colors.qualitative.Set2
                )
                st.plotly_chart(fig3, use_container_width = True)

                #RFM TABLE
                st.markdown("### 📋 Customer Segment Details")
                st.dataframe(
                    rfm_df[["customer_id", "Recency", "Frequency", "Monetary", "Segment"]],
                    use_container_width = True
                )

                #download button
                csv = rfm_df.to_csv(index = False)
                st.download_button(
                    label = "⬇️ Download Segmentation Results",
                    data = csv,
                    file_name = "customer_segments.csv",
                    mime = "text/csv"
                )

            except Exception as e:
                st.error(f"Segmentation failed: {e}")

    st.markdown("---")

    #MODEL 02: Sales Prediction
    st.markdown("## 📈 Model 02: Sales Prediction")
    st.markdown("Predicts the next 3 months of sales based on historical trends using Linear Regression.")

    if st.button("🚀 Run Sales Prediction"):
        with st.spinner("Running prediction..."):
            try:
                monthly_df, forecast_df, metrics, model = run_prediction(df)

                st.success("✅ Prediction Complete!")

                # Model Metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("R² Score", metrics["R2 Score"],
                              help = "Closer to 1.0 = better model")
                
                with col2:
                    st.metric("RMSE", f"₹{metrics['RMSE']:,}",
                               help = "Average prediction error in ₹")
                
                with col3:
                    st.metric("Months of History", len(monthly_df))

                st.markdown("---")

                #Line Chart actual + forecast
                st.markdown("### 📈 Sales History + 3 Month Forecast")

                #combine actual and forecast for chart
                actual_chart = monthly_df[["month_str", "total_sales", "type"]].copy()
                forecast_chart = forecast_df[["month_str", "total_sales", "type"]].copy()
                combined = pd.concat([actual_chart, forecast_chart], ignore_index = True)

                fig4 = px.line(
                    combined,
                    x = "month_str",
                    y = "total_sales",
                    color = "type",
                    markers = True,
                    color_discrete_map = {
                        "Actual" : "#4CAF50",
                        "Forecast" : "#FF6B6B"
                    }
                )
                fig4.update_layout(
                    xaxis_title = "Month",
                    yaxis_title = "Total Sales (₹)"
                )
                st.plotly_chart(fig4, use_container_width = True)

                # Forecast Table
                st.markdown("### 🔮 3 Month Forecast")
                forecast_display = forecast_df[["month_str", "total_sales"]].copy()
                forecast_display.columns = ["Month", "Predicted Sales (₹)"]
                st.dataframe(forecast_display, use_container_width = True)

            except Exception as e:
                st.error(f"Prediction failed: {e}")
            
    st.markdown("---")

    # Model 03: Anomaly Detection
    st.markdown("## 🚨 Model 03: Anomaly Detection")
    st.markdown("Finds suspicious or unusual orders in your sales data using Isolation Forest.")

    contamination = st.slider(
        "Sensitivity (% of orders expected to be suspicious)",
        min_value = 0.01,
        max_value = 0.20,
        value = 0.05,
        step = 0.01,
        help = "Higher = more orders flagged as suspicious"
    )

    if st.button("🚀 Run Anomaly Detection"):
        with st.spinner("Scanning for anomalies..."):
            try:
                result_df, anomaly_df, summary = run_anomaly_detection(df, contamination)

                st.success("✅ Anomaly detection complete!")

                # Metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Orders Scanned", summary["total_orders"])
                with col2:
                    st.metric("✅ Normal Orders", summary["normal_orders"])
                with col3:
                    st.metric("🚨 Suspicious Orders", summary["suspicious_orders"])

                st.markdown("---")

                #Scatter Plot: Normal VS Suspicious
                st.markdown("### 🔍 Order Analysis - Normal VS Suspicious")
                fig5 = px.scatter(
                    result_df,
                    x = "quantity",
                    y = "total_amount",
                    color = "anomaly_label",
                    size = "total_amount",
                    hover_data = ["order_id", "unit_price"],
                    color_discrete_map = {
                        "✅ Normal" : "#4CAF50",
                        "🚨 Suspicious" : "#f44336"
                    }
                )
                fig5.update_layout(
                    xaxis_title = "Quantity",
                    yaxis_title = "Total Amount (₹)"
                )
                st.plotly_chart(fig5, use_container_width = True)

                #suspicious orders table
                if len(anomaly_df) > 0:
                    st.markdown("### 🚨 Suspicious Orders")
                    st.warning(f"Found {len(anomaly_df)} suspicious orders:")
                    st.dataframe(
                        anomaly_df[[
                            "order_id", "customer_id", "order_date",
                            "quantity", "unit_price", "total_amount",
                            "anomaly_label"
                        ]],
                        use_container_width = True
                    )

                    # downlaod
                    csv = anomaly_df.to_csv(index = False)
                    st.download_button(
                        label = "⬇️ Download Suspicious Orders",
                        data = csv,
                        file_name = "suspicious_orders.csv",
                        mime = "text/csv"
                    )
                else:
                    st.success("🥳 No Suspicious Orders Found!!")

            except Exception as e:
                st.error(f"Anomaly Detection Failed: {e}")

elif page == "🧠 Ask DALE":
    st.title("🧠 Ask DALE (AI Assistant)")
    st.markdown("Ask questions in natural language")


    from ai.nl_to_sql import generate_sql, run_query

    user_input = st.text_input("Ask your data...")
    st.markdown("💡 Try: top customers, total sales, category-wise sales")

    if st.button("Run Query"):
        if user_input.strip() == "":
            st.warning("Please enter a query")
        else:
            sql = generate_sql(user_input)

            st.markdown("### 📃 Generated SQL")
            st.code(sql, language='sql')

            try:
                result = run_query(sql)

                st.markdown("### 📊 Result")
                st.dataframe(result)
            
            except Exception as e:
                st.error(f"Error: {e}")