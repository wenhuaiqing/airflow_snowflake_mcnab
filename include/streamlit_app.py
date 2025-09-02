import streamlit as st
import pandas as pd
from datetime import datetime
from snowflake.snowpark.context import get_active_session

session = get_active_session()

# Load construction analytics data
enriched_activities_df = session.table("ETL_DEMO.DEV.ENRICHED_activities").to_pandas()
project_cost_analysis_df = session.table("ETL_DEMO.DEV.PROJECT_COST_ANALYSIS").to_pandas()
contractor_performance_df = session.table("ETL_DEMO.DEV.CONTRACTOR_PERFORMANCE").to_pandas()
material_usage_analysis_df = session.table("ETL_DEMO.DEV.MATERIAL_USAGE_ANALYSIS").to_pandas()
project_timeline_analysis_df = session.table("ETL_DEMO.DEV.PROJECT_TIMELINE_ANALYSIS").to_pandas()

enriched_activities_df['ACTIVITY_DATE'] = pd.to_datetime(enriched_activities_df['ACTIVITY_DATE'])

today = datetime.now().date() 
start_of_month = today.replace(day=1)  
start_of_year = today.replace(month=1, day=1)  

# Calculate key metrics
activities_today = len(enriched_activities_df[enriched_activities_df['ACTIVITY_DATE'].dt.date == today])
total_cost_today = enriched_activities_df[enriched_activities_df['ACTIVITY_DATE'].dt.date == today]['COST'].sum()
total_cost_this_month = enriched_activities_df[enriched_activities_df['ACTIVITY_DATE'].dt.date >= start_of_month]['COST'].sum()
total_cost_this_year = enriched_activities_df[enriched_activities_df['ACTIVITY_DATE'].dt.date >= start_of_year]['COST'].sum()

# Latest activities
latest_activities = enriched_activities_df.sort_values(by='ACTIVITY_DATE', ascending=False).head(5)[[
    'ACTIVITY_DATE', 'PROJECT_NAME', 'CONTRACTOR_NAME', 'ACTIVITY_TYPE', 'COST'
]]

st.set_page_config(page_title="Construction Analytics Dashboard", layout="wide")
st.title("🏗️ Construction Analytics Dashboard")

st.markdown("## Key Metrics")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(label="Activities Today", value=activities_today)
with col2:
    st.metric(label="Cost Today", value=f"${total_cost_today:,.2f}")
with col3:
    st.metric(label="Cost This Month", value=f"${total_cost_this_month:,.2f}")
with col4:
    st.metric(label="Cost This Year", value=f"${total_cost_this_year:,.2f}")

st.markdown("---")
st.markdown("## 🔨 5 Latest Activities")
st.table(latest_activities)

st.markdown("---")
st.markdown("## 📊 Project Cost Analysis")
st.bar_chart(project_cost_analysis_df.set_index('PROJECT_NAME')['TOTAL_COST'])

st.markdown("---")
st.markdown("## 👷 Contractor Performance")
col1, col2 = st.columns([2, 1])
with col1:
    st.bar_chart(contractor_performance_df.set_index('CONTRACTOR_NAME')['TOTAL_COST'])
with col2:
    st.dataframe(contractor_performance_df[['CONTRACTOR_NAME', 'CONTRACTOR_TYPE', 'TOTAL_PROJECTS', 'COMPLETION_RATE_PERCENTAGE']])

st.markdown("---")
st.markdown("## 🔨 Material Usage Analysis")
st.dataframe(material_usage_analysis_df[['MATERIAL_NAME', 'MATERIAL_CATEGORY', 'TOTAL_QUANTITY_USED', 'TOTAL_COST', 'USAGE_FREQUENCY_PERCENTAGE']])

st.markdown("---")
st.markdown("## 📈 Project Timeline Analysis")
col1, col2 = st.columns([2, 1])
with col1:
    st.bar_chart(project_timeline_analysis_df.set_index('PROJECT_NAME')['PROJECT_DURATION_DAYS'])
with col2:
    st.dataframe(project_timeline_analysis_df[['PROJECT_NAME', 'PROJECT_STATUS', 'COMPLETION_PERCENTAGE', 'TIMELINE_VARIANCE_DAYS']])

st.markdown("---")
st.markdown("## 🎯 Project Status Distribution")
project_status_counts = project_cost_analysis_df['PROJECT_STATUS'].value_counts()
st.bar_chart(project_status_counts)

st.markdown("---")
st.markdown("## 💰 Cost by Project Type")
project_type_costs = project_cost_analysis_df.groupby('PROJECT_TYPE')['TOTAL_COST'].sum()
st.bar_chart(project_type_costs)

st.markdown("---")
st.markdown("© 2024 Construction Analytics Inc.")
