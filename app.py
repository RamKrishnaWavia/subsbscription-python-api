import streamlit as st
import pandas as pd
import os
import glob
from datetime import datetime

# --- PAGE SETUP ---
st.set_page_config(page_title="BBD Dashboard Pro", layout="wide")
st.title("🚀 BBD Daily Summary Dashboard")

def load_file(keyword):
    """Search for files matching the keyword (case-insensitive)"""
    all_files = os.listdir('.')
    matches = [f for f in all_files if keyword.lower() in f.lower() and f.endswith(('.csv', '.xlsx'))]
    
    if not matches:
        return None
    
    target = matches[0]
    try:
        if target.endswith('.csv'):
            return pd.read_csv(target)
        else:
            return pd.read_excel(target, engine='openpyxl')
    except Exception as e:
        st.error(f"Error loading {target}: {e}")
        return None

def get_time_condition(series, time_str):
    """Helper to check if delivery time is before a certain threshold"""
    threshold = datetime.strptime(time_str, '%H:%M').time()
    return pd.to_datetime(series).dt.time < threshold

# --- MAIN PROCESSING LOGIC ---
if st.button("Generate Complete Dashboard"):
    with st.spinner("Crunching numbers for all metrics..."):
        # 1. Load Datasets
        df_ord = load_file("order_Report_SA_ID")
        df_sales = load_file("order_sku_sales_bb2")
        df_lmd = load_file("iot-rate-card-iot_orderwise")
        
        if df_ord is None:
            st.error("❌ Critical Error: 'order_Report_SA_ID' file not found.")
        else:
            # --- PRE-PROCESSING ---
            df_ord['delivery_date'] = pd.to_datetime(df_ord['delivery_date'], errors='coerce').dt.normalize()
            # Clean numeric columns
            for col in ['OriginalQty', 'finalquantity', 'OriginalOrderValue', 'FinalOrderValue']:
                df_ord[col] = pd.to_numeric(df_ord[col], errors='coerce').fillna(0)

            # Filter for delivered orders only for delivery metrics
            delivered_mask = df_ord['order_status'].isin(['complete', 'delivered'])

            # --- AGGREGATION ---
            # Group by Date and Store
            grouped = df_ord.groupby(['delivery_date', 'sa_name'])
            
            summary = grouped.agg(
                unique_customers=('member_id', 'nunique'),
                total_orders=('order_id', 'nunique'),
                orders_delivered=('order_id', lambda x: df_ord.loc[x.index & df_ord[delivered_mask].index, 'order_id'].nunique()),
                sub_orders=('Type', lambda x: (x == 'Subscription').sum()),
                topup_orders=('Type', lambda x: (x == 'Topup').sum()),
                # Quantity Metrics (Ordered)
                total_ordered_qty=('OriginalQty', 'sum'),
                sub_qty=('OriginalQty', lambda x: df_ord.loc[x.index][df_ord['Type'] == 'Subscription']['OriginalQty'].sum()),
                topup_qty=('OriginalQty', lambda x: df_ord.loc[x.index][df_ord['Type'] == 'Topup']['OriginalQty'].sum()),
                milk_qty_ordered=('OriginalQty', lambda x: df_ord.loc[x.index][df_ord['Milk / NM'] == 'Milk']['OriginalQty'].sum()),
                non_milk_qty_ordered=('OriginalQty', lambda x: df_ord.loc[x.index][df_ord['Milk / NM'] == 'Non-Milk']['OriginalQty'].sum()),
                # Quantity Metrics (Delivered)
                milk_qty_delivered=('finalquantity', lambda x: df_ord.loc[x.index & df_ord[delivered_mask].index][df_ord['Milk / NM'] == 'Milk']['finalquantity'].sum()),
                non_milk_qty_delivered=('finalquantity', lambda x: df_ord.loc[x.index & df_ord[delivered_mask].index][df_ord['Milk / NM'] == 'Non-Milk']['finalquantity'].sum()),
                total_delivered_qty=('finalquantity', lambda x: df_ord.loc[x.index & df_ord[delivered_mask].index]['finalquantity'].sum()),
                # Cancellations
                oos_cancellations=('cancellation_reason', lambda x: x.str.contains('OOS|stock', case=False, na=False).sum()),
                cx_cancellations=('cancellation_reason', lambda x: x.str.contains('customer', case=False, na=False).sum())
            ).reset_index()

            # --- CALCULATED FIELDS ---
            summary['orders_undelivered'] = summary['total_orders'] - summary['orders_delivered']
            
            # Fill Rates
            summary['fill_rate_milk'] = (summary['milk_qty_delivered'] / summary['milk_qty_ordered']).fillna(0)
            summary['fill_rate_non_milk'] = (summary['non_milk_qty_delivered'] / summary['non_milk_qty_ordered']).fillna(0)
            summary['overall_fill_rate'] = (summary['total_delivered_qty'] / summary['total_ordered_qty']).fillna(0)

            # --- MERGE SALES ---
            if df_sales is not None:
                df_sales['delivery_date'] = pd.to_datetime(df_sales['delivery_date'], errors='coerce').dt.normalize()
                sales_agg = df_sales.groupby(['delivery_date', 'sa_name'])['total_sales'].sum().reset_index()
                summary = pd.merge(summary, sales_agg, on=['delivery_date', 'sa_name'], how='left')
            else:
                summary['total_sales'] = 0

            # ABV & ABQ
            summary['abv'] = (summary['total_sales'] / summary['orders_delivered']).fillna(0)
            summary['abq'] = (summary['total_delivered_qty'] / summary['orders_delivered']).fillna(0)

            # --- MERGE LMD (OTD & Routes) ---
            if df_lmd is not None:
                df_lmd['dt'] = pd.to_datetime(df_lmd['order_delivered_time'], errors='coerce').dt.normalize()
                df_lmd_valid = df_lmd.dropna(subset=['order_delivered_time'])
                
                lmd_agg = df_lmd_valid.groupby(['dt', 'sa_name']).agg(
                    otd_700=('order_delivered_time', lambda x: get_time_condition(x, '07:00').mean()),
                    otd_730=('order_delivered_time', lambda x: get_time_condition(x, '07:30').mean()),
                    otd_800=('order_delivered_time', lambda x: get_time_condition(x, '08:00').mean()),
                    total_routes=('route_id', 'nunique')
                ).reset_index()
                summary = pd.merge(summary, lmd_agg, left_on=['delivery_date', 'sa_name'], right_on=['dt', 'sa_name'], how='left')

            # --- FINAL FORMATTING ---
            final_columns = {
                'delivery_date': 'Date', 
                'sa_name': 'Store Name',
                'unique_customers': 'Total Ordered Customers (Unique)',
                'total_orders': 'Total Orders', 
                'orders_delivered': 'Orders Delivered',
                'sub_orders': 'Subscription Orders', 
                'topup_orders': 'Top-up Orders',
                'orders_undelivered': 'Orders Undelivered',
                'cx_cancellations': 'Cancelled Orders by Customer',
                'oos_cancellations': 'Undelivered Orders Due to OOS',
                'total_ordered_qty': 'Total Ordered Quantity',
                'sub_qty': 'Subscription Quantity',
                'milk_qty_ordered': 'Milk Quantity (Ordered)',
                'non_milk_qty_ordered': 'Non-Milk Quantity (Ordered)',
                'topup_qty': 'Topup Quantity',
                'milk_qty_delivered': 'Milk Quantity (Delivered)',
                'non_milk_qty_delivered': 'Non-Milk Quantity (Delivered)',
                'otd_700': 'On-Time Delivery (Before 7:00 AM)',
                'otd_730': 'On-Time Delivery (Before 7:30 AM)',
                'otd_800': 'On-Time Delivery (Before 8:00 AM)',
                'abv': 'ABV',
                'abq': 'ABQ',
                'fill_rate_milk': 'Fill Rate – Milk',
                'fill_rate_non_milk': 'Fill Rate – Non-Milk',
                'overall_fill_rate': 'Overall Fill Rate',
                'total_sales': 'Sale(₹)',
                'total_routes': 'Total Routes'
            }
            
            # Select and rename
            final_df = summary[list(final_columns.keys())].rename(columns=final_columns)
            final_df = final_df.fillna(0)

            # Display
            st.success("✅ Complete Dashboard Generated!")
            st.dataframe(final_df, use_container_width=True)

            # Download
            csv = final_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Download Final Report", data=csv, file_name="full_bbd_dashboard.csv")
