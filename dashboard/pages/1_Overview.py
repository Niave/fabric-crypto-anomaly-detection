import streamlit as st
from utils.queries import get_kpis
from utils.formatting import summarize_metrics
from utils.config import DEFAULT_START_DATE, DEFAULT_END_DATE

st.title("📈 Overview")

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



df_kpis = get_kpis(start_date, end_date)


if not df_kpis.empty:
    users = int(df_kpis["USERS"].iloc[0])
    sessions = int(df_kpis["SESSIONS"].iloc[0])
    avg_duration_val = df_kpis["AVG_SESSION_DURATION"].iloc[0]
    avg_duration = float(avg_duration_val) if avg_duration_val is not None else 0.0
    


    col1, col2, col3, col4 = st.columns(4)
    col1.metric("👥 Users", f"{users:,}")
    col2.metric("📊 Sessions", f"{sessions:,}")
    col4.metric("⏱️ Avg. Session Duration", f"{avg_duration:.2f} min")

    st.subheader("📝 Summary")
    st.success(summarize_metrics(users, sessions, avg_duration))

    csv = df_kpis.to_csv(index=False).encode("utf-8")
    st.download_button("📥 Download KPIs", csv, "overview_kpis.csv", "text/csv")
else:
    st.warning("No data for selected range.")

