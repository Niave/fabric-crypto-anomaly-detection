import streamlit as st
from utils.queries import get_user_behavior
import plotly.express as px
from utils.config import DEFAULT_START_DATE, DEFAULT_END_DATE
import pandas as pd

st.title("📊 User Behavior")

st.markdown("""
This dashboard shows the distribution of users based on their total event counts and conversion rates.

- The first chart groups users by the number of events they generated, helping identify activity levels.
- The second chart shows how many users fall into different conversion rate brackets.
""")


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

df = get_user_behavior(start_date, end_date)

if df.empty:
    st.warning("No user data available for selected range.")
else:
    st.subheader("User Events and Conversion")

    # 🛠️ Fix column types
    df["TOTAL_EVENTS"] = pd.to_numeric(df["TOTAL_EVENTS"], errors="coerce")
    df["CONVERSION_RATE"] = pd.to_numeric(df["CONVERSION_RATE"], errors="coerce")
    
    

    # 📊 Plot histograms
    fig1 = px.histogram(df["TOTAL_EVENTS"],nbins=20, title="Distribution of Users by Their Total Number of Events")
    fig2 = px.histogram(df["CONVERSION_RATE"], nbins=20, title="User Conversion Rate Distribution")
    fig1.update_layout(xaxis_title="Total Events", yaxis_title="Number of Users")
    fig2.update_layout(xaxis_title="Conversion Rate", yaxis_title="Number of Users")
    st.plotly_chart(fig1, use_container_width=True)
    st.plotly_chart(fig2, use_container_width=True)
    # 🧾 Show table and download
    st.dataframe(df)

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("📥 Download CSV", csv, "user_behavior.csv", "text/csv")
