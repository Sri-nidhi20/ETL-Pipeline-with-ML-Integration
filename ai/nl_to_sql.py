import streamlit as st
from google import genai
from sqlalchemy import create_engine
import pandas as pd
from etl.config import DATABASE_URL

client = genai.Client(api_key = st.secrets["GEMINI_API_KEY"])

def generate_sql(user_query):
    query = user_query.lower()

    if "top customers" in query:
        return """
        SELECT customer_id, SUM(total_amount) AS total_spent
        FROM sales_clean
        GROUP BY customer_id
        ORDER BY total_spent DESC
        LIMIT 5;
        """
    elif "total sales" in query:
        return """
        SELECT SUM(total_amount) AS total_sales
        FROM sales_clean;
        """
    elif "category" in query:
        return """
        SELECT category, SUM(total_amount) AS total_sales
        FROM sales_clean
        GROUP BY category;
        """
    elif "region" in query:
        return """
        SELECT region, SUM(total_amount) AS total_sales
        FROM sales_clean
        GROUP BY region;
        """
    elif "recent orders" in query:
        return """
        SELECT * 
        FROM sales_clean
        ORDER BY order_date DESC
        LIMIT 10;
        """
    else:
        return "SELECT * FROM sales_clean LIMIT 10;"

def run_query(sql):
    engine = create_engine(DATABASE_URL)
    return pd.read_sql(sql, engine)