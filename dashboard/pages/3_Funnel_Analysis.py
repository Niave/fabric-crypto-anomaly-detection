import streamlit as st
from utils.queries import get_funnel_metrics
import plotly.graph_objects as go
from utils.config import DEFAULT_START_DATE, DEFAULT_END_DATE

st.title("🛍 Funnel Analysis")
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


funnel = get_funnel_metrics(start_date, end_date)

st.subheader("Conversion Funnel")
fig = go.Figure(go.Funnel(
    y=["Product Views", "Add to Cart", "Purchases"],
    x=[funnel["VIEWS"], funnel["ADD_TO_CART"], funnel["PURCHASES"]]
))
st.plotly_chart(fig)

with st.expander("ℹ️ Chart Details"):
    st.markdown("The funnel shows how users move from viewing products to purchasing.")