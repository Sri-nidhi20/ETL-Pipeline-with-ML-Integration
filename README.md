# Automated Incremental ETL Pipeline with Data Quality Validation and ML Integration

**BCA Final Year Project**

---

## 📌 Project Overview

This project simulates how real organizations prepare raw data before analysts use it for business insights.

The system automates:
- Extracting raw e-commerce sales data from CSV files
- Validating and cleaning the data
- Loading only new records incrementally into a PostgreSQL database
- Applying machine learning models for analytics

---

## 🏗️ Architecture
```
CSV Upload (Any e-commerce dataset)
         ↓
ETL Pipeline
  ├── extract.py      → Read CSV
  ├── validate.py     → Check data quality
  ├── transform.py    → Clean and format
  ├── incremental.py  → Filter new records only
  └── load.py         → Insert into PostgreSQL
         ↓
PostgreSQL Database
  ├── sales_clean       → Cleaned sales data
  ├── etl_run_log       → Pipeline run history
  └── data_quality_log  → Quality metrics per run
         ↓
Machine Learning
  ├── Customer Segmentation  (KMeans Clustering)
  ├── Sales Prediction       (Linear Regression)
  └── Anomaly Detection      (Isolation Forest)
         ↓
Streamlit Dashboard
  ├── Home & Upload
  ├── Data Quality Report
  ├── View Clean Data
  ├── ETL Run Logs
  └── ML Analytics
```

---

## 🛠️ Technologies Used

| Technology | Purpose |
|------------|---------|
| Python | Core programming language |
| Pandas | Data manipulation and cleaning |
| PostgreSQL | Relational database storage |
| SQLAlchemy | Python-PostgreSQL connection |
| Scikit-learn | Machine learning models |
| Streamlit | Web frontend dashboard |
| Plotly | Interactive charts |
| Python-dotenv | Environment variable management |

---

## 📁 Folder Structure
```
etl_ml_project/
├── .env                  # Database credentials (not uploaded)
├── .env.example          # Template for credentials
├── requirements.txt      # All Python dependencies
├── README.md             # Project documentation
├── data/
│   └── sample_data.csv   # Sample dataset for testing
├── etl/
│   ├── config.py         # Database connection config
│   ├── extract.py        # CSV reading module
│   ├── validate.py       # Data quality validation
│   ├── transform.py      # Data cleaning and formatting
│   ├── incremental.py    # New records filter logic
│   ├── load.py           # PostgreSQL insert + logging
│   └── main.py           # Pipeline orchestrator
├── ml/
│   ├── segmentation.py   # KMeans customer segmentation
│   ├── prediction.py     # Linear regression sales forecast
│   └── anomaly.py        # Isolation forest anomaly detection
├── app/
│   └── streamlit_app.py  # Streamlit web dashboard
└── logs/
```

---

## ⚙️ Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/Sri-nidhi20/etl_ml_project.git
cd etl_ml_project
```

### 2. Create Virtual Environment
```bash
python -m venv .venv
.venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Database
Create a `.env` file in the root folder:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ETL_PROJECT
DB_USER=postgres
DB_PASSWORD=your_password_here
```

### 5. Create PostgreSQL Tables
Run this SQL in pgAdmin on your `ETL_PROJECT` database:
```sql
CREATE TABLE IF NOT EXISTS sales_clean (
    order_id        VARCHAR(50) PRIMARY KEY,
    customer_id     VARCHAR(50),
    order_date      DATE,
    product_id      VARCHAR(50),
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    quantity        INTEGER,
    unit_price      NUMERIC(10, 2),
    total_amount    NUMERIC(10, 2),
    region          VARCHAR(100),
    payment_method  VARCHAR(50),
    status          VARCHAR(50),
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id          SERIAL PRIMARY KEY,
    run_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rows_extracted  INTEGER,
    rows_cleaned    INTEGER,
    rows_inserted   INTEGER,
    rows_rejected   INTEGER,
    status          VARCHAR(20),
    notes           TEXT
);

CREATE TABLE IF NOT EXISTS data_quality_log (
    log_id              SERIAL PRIMARY KEY,
    run_timestamp       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_rows          INTEGER,
    missing_order_id    INTEGER,
    duplicate_rows      INTEGER,
    invalid_dates       INTEGER,
    negative_values     INTEGER,
    rows_passed         INTEGER
);
```

### 6. Run the Application
```bash
streamlit run app/streamlit_app.py
```

Open your browser at `http://localhost:8501`

---

## 🚀 How to Use

1. Go to **Home & Upload** page
2. Upload any e-commerce CSV file
3. Map your columns to the system columns
4. Click **Run ETL Pipeline**
5. View **Data Quality Report** for validation results
6. View **Clean Data** to explore the processed records
7. Go to **ML Models** to run analytics

---

## 🤖 Machine Learning Models

### 1. Customer Segmentation (KMeans)
Groups customers using RFM analysis:
- **Recency** — Days since last purchase
- **Frequency** — Number of orders
- **Monetary** — Total amount spent

Segments: 💎 High Value, 🟢 Loyal, 🟡 At Risk, 🔴 Lost

### 2. Sales Prediction (Linear Regression)
- Aggregates historical monthly sales
- Predicts next 3 months revenue
- Shows R² score and RMSE metrics

### 3. Anomaly Detection (Isolation Forest)
- Scans all orders for unusual patterns
- Flags suspicious quantity, price, or total amount values
- Adjustable sensitivity slider

---

## 📊 Key Features

- ✅ Upload **any** e-commerce CSV — flexible column mapping
- ✅ Incremental loading — never inserts duplicate records
- ✅ Data quality validation with detailed reports
- ✅ Interactive charts with Plotly
- ✅ Full ETL run history logging
- ✅ Download cleaned data and ML results as CSV

---

## 👨‍💻 Developer

**Name:** Kukutam Srinidhi  
**College:** AIMS Degree College 
**Course:** BCA Final Year  
**Year:** 2023 - 2026
```

---

## Step 26: Create `.env.example`

Create a file named `.env.example` in your root folder:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ETL_PROJECT
DB_USER=postgres
DB_PASSWORD=your_password_here