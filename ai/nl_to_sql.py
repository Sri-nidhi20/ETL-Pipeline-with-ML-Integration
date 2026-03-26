import streamlit as st
import google.generativeai as genai
from sqlalchemy import create_engine
import pandas as pd
from etl.config import DATABASE_URL

genai.configure(api_key = st.secrets["GEMINI_API_KEY"])

def generate_sql(user_query):
    prompt = f"""
    You are an SQL Expert.
     
    Convert the following natural language query into a PostgreSQL SQL query.
       
    Table: sales_clean
    Columns: order_id, customer_id, order_date, product_id, product_name, category, quantity, unit_price, total_amount, region, payment_method, status, loaded_at
    
    Rules:
    - Only use SELECT queries
    - Do Not modify database
    - Return ONLy SQL query (no explanation)
    
    Query: {user_query}
    """

    model = genai.GenerativeModel("gemini-1.5-flash")
    response = model.generate_content(prompt)

    return response.text.strip()

def run_query(sql):
    engine = create_engine(DATABASE_URL)
    return pd.read_sql(sql, engine)