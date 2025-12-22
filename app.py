import streamlit as st
import pandas as pd
import os
import glob

# --- PAGE SETUP ---
st.set_page_config(page_title="BBD Dashboard", layout="wide")
st.title("🚀 BBD Daily Summary Dashboard")

def load_file(keyword):
    """Search for files matching the keyword (case-insensitive)"""
    all_files = os.listdir('.')
    matches = [f for f in all_files if keyword.lower() in f.lower() and f.endswith(('.csv', '.xlsx'))]
    
    if not matches:
        return None
    
    target = matches[0] # Pick the first match
    try:
        if target.endswith('.csv'):
            return pd.read_csv(target)
        else:
            return pd.read_excel(target, engine='openpyxl')
    except Exception as e:
        st.error(f"Error loading {target}: {e}")
        return None

# --- MAIN PROCESSING LOGIC ---
if st.button("Generate Dashboard"):
    with st.spinner("Processing files..."):
        # 1. Load Datasets with correct keywords
        df_ord = load_file("order_Report_SA_ID")
        df_sales = load_file("order_sku_sales_bb2")
        df_lmd = load_file("iot-rate-card-iot_orderwise")
        df_ota = load_file("OTA")
        df_pick = load_file("B2B_ORDER_pICK")

        if df_ord is None:
            st.error("❌ Critical Error: 'order_Report_SA_ID' file not found.")
        else:
            # --- STEP 1: Process Orders ---
            df_ord['delivery_date'] = pd.to_datetime(df_ord['delivery_date'], errors='coerce').dt.normalize()
            
            orders_agg = df_ord.groupby(['delivery_date', 'sa_name']).agg(
                unique_customers=('member_id', 'nunique'),
                total_orders=('order_id', 'nunique'),
                orders_delivered=('order_status', lambda x: x[x.isin(['complete', 'delivered'])].count()),
                sub_orders=('Type', lambda x: (x == 'Subscription').sum()),
                topup_orders=('Type', lambda x: (x == 'Topup').sum()),
                oos_cancellations=('cancellation_reason', lambda x: x.str.contains('OOS|stock', case=False, na=False).sum()),
                cx_cancellations=('cancellation_reason', lambda x: x.str.contains('customer', case=False, na=False).sum())
            ).reset_index()

            # --- STEP 2: Merge Sales ---
            if df_sales is not None:
                df_sales['delivery_date'] = pd.to_datetime(df_sales['delivery_date'], errors='coerce').dt.normalize()
                sales_agg = df_sales.groupby(['delivery_date', 'sa_name'])['total_sales'].sum().reset_index()
                orders_agg = pd.merge(orders_agg, sales_agg, on=['delivery_date', 'sa_name'], how='left')

            # --- STEP 3: Merge LMD Data (OTD & Routes) ---
            if df_lmd is not None:
                df_lmd['dt'] = pd.to_datetime(df_lmd['order_delivered_time'], errors='coerce').dt.normalize()
                # OTD Logic: delivered before 7 AM
                df_lmd['is_otd'] = pd.to_datetime(df_lmd['order_delivered_time']).dt.time < pd.to_datetime('07:00:00').time()
                lmd_agg = df_lmd.groupby(['dt', 'sa_name']).agg(
                    otd_perc=('is_otd', 'mean'),
                    total_routes=('route_id', 'nunique')
                ).reset_index()
                orders_agg = pd.merge(orders_agg, lmd_agg, left_on=['delivery_date', 'sa_name'], right_on=['dt', 'sa_name'], how='left')

            # --- STEP 4: FINAL CLEANUP (The fillna Fix) ---
            # Explicitly fill only numeric columns to avoid FutureWarnings
            numeric_cols = orders_agg.select_dtypes(include=['number']).columns
            orders_agg[numeric_cols] = orders_agg[numeric_cols].fillna(0)

            # Rename for final "Day wise Format"
            final_rename = {
                'delivery_date': 'Date', 
                'sa_name': 'Store Name',
                'unique_customers': 'Total Ordered Customers (Unique)',
                'total_orders': 'Total Orders', 
                'orders_delivered': 'Orders Delivered',
                'sub_orders': 'Subscription Orders', 
                'topup_orders': 'Top-up Orders',
                'total_sales': 'Sale(₹)',
                'otd_perc': 'On-Time Delivery (Before 7:00 AM)',
                'total_routes': 'Total Routes'
            }
            orders_agg.rename(columns=final_rename, inplace=True)

            # --- STEP 5: DISPLAY & DOWNLOAD ---
            st.success("✅ Dashboard Generated Successfully!")
            
            # Show the table in the browser
            st.subheader("Data Preview")
            st.dataframe(orders_agg, use_container_width=True)

            # Provide the download button
            csv = orders_agg.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Final Dashboard CSV",
                data=csv,
                file_name="final_daily_summary_dashboard.csv",
                mime="text/csv"
            )

# Sidebar info
st.sidebar.info("Upload your CSV files to the GitHub folder and click 'Generate' to refresh data.")
