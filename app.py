import streamlit as st
import pandas as pd
import numpy as np
import os
from datetime import datetime

# --- PAGE SETUP ---
st.set_page_config(page_title="BBD Daily Summary", layout="wide")
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
            return pd.read_csv(target, low_memory=False)
        else:
            return pd.read_excel(target, engine='openpyxl')
    except Exception as e:
        st.error(f"Error loading {target}: {e}")
        return None

def get_time_mask(series, time_str):
    """Returns a boolean mask for times before the threshold"""
    threshold = datetime.strptime(time_str, '%H:%M').time()
    return pd.to_datetime(series, errors='coerce').dt.time < threshold

# --- MAIN PROCESSING LOGIC ---
if st.button("Generate Complete Dashboard"):
    with st.spinner("Processing files and calculating metrics..."):
        # 1. Load Datasets
        df_ord = load_file("order_Report_SA_ID")
        df_sku = load_file("order_sku_sales_bb2")
        df_lmd = load_file("iot-rate-card-iot_orderwise")
        df_ota = load_file("OTA")
        df_pick = load_file("B2B_ORDER_pICK")

        if df_ord is None:
            st.error("❌ Critical Error: 'order_Report_SA_ID' file not found.")
        else:
            # --- DATA CLEANING & PRE-CALCULATION ---
            df_ord['delivery_date'] = pd.to_datetime(df_ord['delivery_date'], errors='coerce').dt.normalize()
            
            # Numeric conversion to avoid errors
            for col in ['OriginalQty', 'finalquantity', 'OriginalOrderValue', 'FinalOrderValue']:
                df_ord[col] = pd.to_numeric(df_ord[col], errors='coerce').fillna(0)

            # Pre-calculate flags to avoid the "Broadcast ValueError"
            df_ord['is_delivered'] = df_ord['order_status'].str.lower().isin(['complete', 'delivered'])
            df_ord['is_sub'] = df_ord['Type'].str.lower() == 'subscription'
            df_ord['is_topup'] = df_ord['Type'].str.lower() == 'topup'
            df_ord['is_milk'] = df_ord['Milk / NM'].str.lower() == 'milk'
            df_ord['is_non_milk'] = df_ord['Milk / NM'].str.lower() == 'non-milk'
            df_ord['is_oos'] = df_ord['cancellation_reason'].str.contains('OOS|stock', case=False, na=False)
            df_ord['is_cx_cancel'] = df_ord['cancellation_reason'].str.contains('customer', case=False, na=False)

            # --- 1. CORE AGGREGATION (From Order Report) ---
            summary = df_ord.groupby(['delivery_date', 'sa_name']).agg(
                total_customers=('member_id', 'nunique'),
                total_orders=('order_id', 'nunique'),
                orders_delivered=('order_id', lambda x: df_ord.loc[x.index, 'is_delivered'].sum() if 'order_id' in df_ord.columns else 0),
                sub_orders=('is_sub', 'sum'),
                topup_orders=('is_topup', 'sum'),
                oos_cancellations=('is_oos', 'sum'),
                cx_cancellations=('is_cx_cancel', 'sum'),
                # Quantity Metrics
                ordered_qty=('OriginalQty', 'sum'),
                delivered_qty=('finalquantity', 'sum'),
                milk_qty_ordered=('OriginalQty', lambda x: df_ord.loc[x.index[df_ord.loc[x.index, 'is_milk']], 'OriginalQty'].sum()),
                milk_qty_delivered=('finalquantity', lambda x: df_ord.loc[x.index[df_ord.loc[x.index, 'is_milk']], 'finalquantity'].sum()),
                nmilk_qty_ordered=('OriginalQty', lambda x: df_ord.loc[x.index[df_ord.loc[x.index, 'is_non_milk']], 'OriginalQty'].sum()),
                nmilk_qty_delivered=('finalquantity', lambda x: df_ord.loc[x.index[df_ord.loc[x.index, 'is_non_milk']], 'finalquantity'].sum()),
                sub_qty=('OriginalQty', lambda x: df_ord.loc[x.index[df_ord.loc[x.index, 'is_sub']], 'OriginalQty'].sum()),
                topup_qty=('OriginalQty', lambda x: df_ord.loc[x.index[df_ord.loc[x.index, 'is_topup']], 'OriginalQty'].sum())
            ).reset_index()

            # Fix:nunique() counts on boolean sum above were slightly off, correction for unique order counts:
            # We use the flags to count unique IDs where flag is true
            summary['orders_delivered'] = df_ord[df_ord['is_delivered']].groupby(['delivery_date', 'sa_name'])['order_id'].nunique().values if len(df_ord[df_ord['is_delivered']]) > 0 else 0

            # --- 2. LOGISTICS & OTD (From LMD Report) ---
            if df_lmd is not None:
                df_lmd['dt'] = pd.to_datetime(df_lmd['order_delivered_time'], errors='coerce').dt.normalize()
                df_lmd['weight'] = pd.to_numeric(df_lmd['weight'], errors='coerce').fillna(0)
                
                lmd_agg = df_lmd.groupby(['dt', 'sa_name']).agg(
                    otd_700=('order_delivered_time', lambda x: (pd.to_datetime(x).dt.time < datetime.strptime('07:00', '%H:%M').time()).mean()),
                    otd_730=('order_delivered_time', lambda x: (pd.to_datetime(x).dt.time < datetime.strptime('07:30', '%H:%M').time()).mean()),
                    otd_800=('order_delivered_time', lambda x: (pd.to_datetime(x).dt.time < datetime.strptime('08:00', '%H:%M').time()).mean()),
                    total_routes=('route_id', 'nunique'),
                    total_weight=('weight', 'sum')
                ).reset_index()
                summary = pd.merge(summary, lmd_agg, left_on=['delivery_date', 'sa_name'], right_on=['dt', 'sa_name'], how='left')

            # --- 3. SALES (From SKU/Sales Report) ---
            if df_sku is not None:
                df_sku['delivery_date'] = pd.to_datetime(df_sku['delivery_date'], errors='coerce').dt.normalize()
                sales_agg = df_sku.groupby(['delivery_date', 'sa_name'])['total_sales'].sum().reset_index()
                summary = pd.merge(summary, sales_agg, on=['delivery_date', 'sa_name'], how='left')

            # --- CALCULATED METRICS ---
            summary['orders_undelivered'] = summary['total_orders'] - summary['orders_delivered']
            summary['abv'] = (summary['total_sales'] / summary['orders_delivered']).replace([np.inf, -np.inf], 0).fillna(0)
            summary['abq'] = (summary['delivered_qty'] / summary['orders_delivered']).replace([np.inf, -np.inf], 0).fillna(0)
            summary['fr_milk'] = (summary['milk_qty_delivered'] / summary['milk_qty_ordered']).fillna(0)
            summary['fr_non_milk'] = (summary['nmilk_qty_delivered'] / summary['nmilk_qty_ordered']).fillna(0)
            summary['overall_fr'] = (summary['delivered_qty'] / summary['ordered_qty']).fillna(0)
            
            # Weight metrics
            if 'total_weight' in summary.columns:
                summary['weight_per_route'] = (summary['total_weight'] / summary['total_routes']).fillna(0)
                summary['weight_per_order'] = (summary['total_weight'] / summary['orders_delivered']).fillna(0)

            # --- FINAL RENAMING (Day Wise Format) ---
            final_cols = {
                'delivery_date': 'Date', 'sa_name': 'Store Name',
                'total_customers': 'Total Ordered Customers (Unique)', 'total_orders': 'Total Orders',
                'orders_delivered': 'Orders Delivered', 'sub_orders': 'Subscription Orders',
                'topup_orders': 'Top-up Orders', 'orders_undelivered': 'Orders Undelivered',
                'cx_cancellations': 'Cancelled Orders by Customer', 'oos_cancellations': 'Undelivered Orders Due to OOS',
                'ordered_qty': 'Total Ordered Quantity', 'sub_qty': 'Subscription Quantity',
                'milk_qty_ordered': 'Milk Quantity (Ordered)', 'nmilk_qty_ordered': 'Non-Milk Quantity (Ordered)',
                'topup_qty': 'Topup Quantity', 'milk_qty_delivered': 'Milk Quantity (Delivered)',
                'nmilk_qty_delivered': 'Non-Milk Quantity (Delivered)',
                'otd_700': 'On-Time Delivery (Before 7:00 AM)', 'otd_730': 'On-Time Delivery (Before 7:30 AM)',
                'otd_800': 'On-Time Delivery (Before 8:00 AM)',
                'abv': 'ABV', 'abq': 'ABQ',
                'fr_milk': 'Fill Rate – Milk', 'fr_non_milk': 'Fill Rate – Non-Milk',
                'overall_fr': 'Overall Fill Rate', 'total_sales': 'Sale(₹)',
                'total_routes': 'Total Routes', 'weight_per_route': 'Weight/Route', 'weight_per_order': 'Weight/Order'
            }

            final_df = summary[list(final_cols.keys())].rename(columns=final_cols).fillna(0)

            # --- DISPLAY ---
            st.success("✅ Dashboard Successfully Updated!")
            st.dataframe(final_df, use_container_width=True)
            
            csv = final_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Full Dashboard CSV",
                data=csv,
                file_name=f"BBD_Full_Summary_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
