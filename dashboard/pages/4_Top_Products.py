import streamlit as st
from utils.queries import get_top_products
import plotly.express as px
from utils.config import DEFAULT_START_DATE, DEFAULT_END_DATE

st.title("🏆 Top Products")

dates = st.date_input("Select Date Range", [DEFAULT_START_DATE, DEFAULT_END_DATE])

# Normalize to a tuple of two dates
if isinstance(dates, (list, tuple)):
    if len(dates) == 2:
        start_date, end_date = dates
    elif len(dates) == 1:
        start_date = end_date = dates[0]
    else:
        st.warning("Please select at least one date.")
        st.stop()
else:
    start_date = end_date = dates

df = get_top_products(start_date, end_date)

if df.empty:
    st.warning("No product data for selected range.")
else:
    fig = px.bar(df, x="PRODUCT_ID", y="NUM_PURCHASES", title="Top Products by Purchases", color="NUM_PURCHASES")
    st.plotly_chart(fig)

    st.dataframe(df)

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("📥 Download CSV", csv, "top_products.csv", "text/csv")