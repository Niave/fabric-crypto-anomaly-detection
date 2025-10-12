import streamlit as st
from utils.config import BRANDING

st.set_page_config(
    page_title= "Product Analytics Dashboard",
    page_icon= "📊",
    layout= "wide"
)

st.markdown(
    f"<h1 style= 'color: {BRANDING['primary_colour']}'> 📊 Product Analytics Dashboard </h1>",
    unsafe_allow_html= True
)

with st.sidebar:
    st.image("dashboard/assets/logo.png", width= 150)
    st.markdown("Welcome! Use the sidebar to navigate and apply filters.")
    st.markdown("### ℹ️ About this Dashboard")
    st.info(
        "This dashboard helps **Product Managers** and **Data Analysts** track user behavior, "
        "conversion funnels, and product performance using aggregated product & user metrics."
    )