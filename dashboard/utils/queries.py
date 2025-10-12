import pandas as pd 
import snowflake.connector
import streamlit as st
from typing import Dict

@st.cache_resource
def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema="GOLD"
    )

@st.cache_data(ttl=600)
def get_kpis(start_date: str, end_date: str) -> pd.DataFrame:
    conn = get_connection()
    query = f"""
    SELECT
        COUNT(DISTINCT USER_ID) AS users,
        COUNT(DISTINCT SESSION_ID) AS sessions,
        AVG(SESSION_DURATION_MINUTES) AS avg_session_duration
    FROM session_metrics
    WHERE INGESTED_AT BETWEEN '{start_date}' AND '{end_date}'
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=600)
def get_user_behavior(start_date: str, end_date: str) -> pd.DataFrame:
    conn = get_connection()
    query = f"""
    SELECT USER_ID, TOTAL_EVENTS, NUM_PURCHASES, NUM_CLICKS, CONVERSION_RATE
    FROM user_metrics
    WHERE TO_DATE(INGESTED_AT) BETWEEN '{start_date}' AND '{end_date}';
    """
    return pd.read_sql(query, conn)


@st.cache_data(ttl=600)
def get_funnel_metrics(start_date: str, end_date: str) -> Dict[str, int]:
    conn = get_connection()
    query = f"""
    SELECT
        SUM(NUM_VIEWS) AS views,
        SUM(NUM_ADD_TO_CART) AS add_to_cart,
        SUM(NUM_PURCHASES) AS purchases
    FROM product_metrics
    WHERE INGESTED_AT BETWEEN '{start_date}' AND '{end_date}';
    """
    df = pd.read_sql(query, conn)

    if df.empty:
        return {"views": 0, "add_to_cart": 0, "purchases": 0}
    return df.iloc[0].to_dict()

@st.cache_data(ttl=600)
def get_top_products(start_date: str, end_date: str) -> pd.DataFrame:
    conn = get_connection()
    query = f"""
    SELECT PRODUCT_ID, NUM_PURCHASES, NUM_ADD_TO_CART, NUM_VIEWS
    FROM product_metrics
    WHERE INGESTED_AT BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY NUM_PURCHASES DESC
    LIMIT 10;
    """
    return pd.read_sql(query, conn)