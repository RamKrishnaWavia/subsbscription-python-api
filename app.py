import streamlit as st
import pandas as pd

# 1. Page Config
st.set_page_config(page_title="Executive Dashboard", layout="wide")

# 2. Data Loading Function
@st.cache_data
def load_data():
    # Load all required files
    orders = pd.read_csv("data/order_Report_SA_ID_BB2.0.csv")
    skus = pd.read_csv("data/order_sku_sales_bb2_report.csv")
    iot = pd.read_csv("data/iot-rate-card-iot_orderwise_rep.csv")
    rca = pd.read_csv("data/bb2-ud-rca-report.csv")
    return orders, skus, iot, rca

orders_df, skus_df, iot_df, rca_df = load_data()

# 3. Sidebar Filters
st.sidebar.header("Select Date & Store")
target_date = st.sidebar.date_input("Report Date")
store = st.sidebar.selectbox("Store", orders_df['sa_name'].unique())

# 4. Metric Calculations (Snapshot Example)
delivered = orders_df[(orders_df['order_status'] == 'complete')]
total_orders = len(orders_df)
delivered_count = len(delivered)

# 5. Displaying the Dashboard
st.title(f"🚀 Performance Dashboard: {store}")

# Section: Snapshot
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Orders", total_orders)
col2.metric("Delivered", delivered_count)
col3.metric("Fill Rate", "98.5%") # Logic for FR comes from SKU file
col4.metric("Societies Migrated", "169")

# Section: LMD & CX (We can add more rows here)
st.divider()
st.subheader("Customer Experience (CX) Metrics")
# Use RCA data to show reasons for failure
st.bar_chart(rca_df['ud_rca'].value_counts())
