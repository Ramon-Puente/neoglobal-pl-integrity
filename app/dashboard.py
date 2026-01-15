import streamlit as st
import pandas as pd
import plotly.express as px
import duckdb

# Set Streamlit page configuration
st.set_page_config(layout="wide", page_title="NeoGlobal P&L Integrity Dashboard", page_icon="📊")

# --- Database Connection ---
@st.cache_resource
def get_duckdb_connection():
    """Establishes and caches a DuckDB connection."""
    conn = duckdb.connect(database='/mnt/ssd_warehouse/neoglobal.db', read_only=True)
    return conn

conn = get_duckdb_connection()

# --- Data Loading ---
@st.cache_data(ttl=600) # Cache data for 10 minutes
def load_data():
    """Loads reconciliation data from DuckDB."""
    query = """
    SELECT 
        reconciliation_id,
        stripe_amount,
        netsuite_amount,
        is_missing_in_erp,
        is_amount_mismatch,
        is_perfect_match
    FROM neoglobal.fct_reconciliation
    """
    df = conn.execute(query).fetchdf()
    
    # Calculate variance
    df['variance'] = df['stripe_amount'].fillna(0) - df['netsuite_amount'].fillna(0)
    
    return df

df = load_data()

# --- KPI Calculations ---
total_records = len(df)
perfect_matches_count = df['is_perfect_match'].sum()
mismatched_records_count = df['is_amount_mismatch'].sum()
missing_in_erp_count = df['is_missing_in_erp'].sum()

# Total Exposure ($) - sum of absolute variances for non-perfect matches
total_exposure = df[~df['is_perfect_match']]['variance'].abs().sum()

# Data Integrity Score %
data_integrity_score = (perfect_matches_count / total_records) * 100 if total_records > 0 else 0

# Month-End Time Saved (Hardcoded)
month_end_time_saved = "4 Days"


# --- Dashboard UI ---
st.title("📊 NeoGlobal P&L Integrity Executive Dashboard")

st.markdown("""
    This dashboard provides a high-level overview of the P&L integrity,
    highlighting variances and data quality issues between Stripe and NetSuite records.
""")

# KPI Row
col1, col2, col3 = st.columns(3)

with col1:
    st.metric(label="Total Exposure ($)", value=f"${total_exposure:,.2f}")

with col2:
    st.metric(label="Data Integrity Score %", value=f"{data_integrity_score:.2f}%")

with col3:
    st.metric(label="Month-End Time Saved", value=month_end_time_saved)

st.markdown("---")

# Visuals
st.header("Reconciliation Overview")

# Prepare data for pie chart
pie_data = pd.DataFrame({
    'Category': ['Perfect Matches', 'Mismatches/Missing Records'],
    'Count': [perfect_matches_count, mismatched_records_count + missing_in_erp_count]
})

fig = px.pie(
    pie_data,
    values='Count',
    names='Category',
    title='Distribution of Reconciliation Status',
    color_discrete_sequence=px.colors.sequential.RdBu,
    hole=0.3
)
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# Forensic Table
st.header("Forensic Review: Anomalies for Auditor Review")

# Filter for non-perfect matches (anomalies)
anomalies_df = df[~df['is_perfect_match']].copy()

if not anomalies_df.empty:
    st.dataframe(
        anomalies_df,
        use_container_width=True,
        hide_index=True,
        height=400  # Adjust height as needed
    )
else:
    st.info("No anomalies detected! All records are perfectly reconciled.")

st.markdown("---")
st.caption("Data last updated from `marts.fct_reconciliation`.")