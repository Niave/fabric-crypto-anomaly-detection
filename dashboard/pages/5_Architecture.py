import streamlit as st

st.title("🧱 Pipeline Architecture")

st.image("dashboard/assets/arc.png", use_container_width=True)

st.markdown("""
### 🧠 Business Context
This dashboard uses aggregated gold-level metrics optimized for product analytics.

### 🔄 Pipeline Stages
- 🧪 Raw event & session data ingested from CSV and JSON files
- ⬇️ Transformation & aggregation into gold tables (product_metrics, user_metrics, session_metrics)
- 🐳 Dockerized deployment & infrastructure managed with Terraform
- 📊 Interactive Streamlit dashboard with multi-page navigation

> The stack showcases modern analytics best practices: modularity, caching, and clean UI.
""")
