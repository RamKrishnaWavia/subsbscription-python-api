import pandas as pd
import glob
import os

# --- CONFIGURATION ---
# Path where your daily raw files are stored (use "." for the current folder)
DATA_FOLDER = "." 
OUTPUT_FILE = "final_daily_summary_dashboard.csv"

def load_file(keyword):
    """Finds a file by keyword and loads it regardless of format (.csv, .xlsx, .xls)"""
    # Look for any file containing the keyword with supported extensions
    extensions = ['*.csv', '*.xlsx', '*.xls']
    found_files = []
    for ext in extensions:
        found_files.extend(glob.glob(os.path.join(DATA_FOLDER, f"*{keyword}*{ext}")))
    
    if not found_files:
        print(f"Warning: No file found for keyword '{keyword}'")
        return None
    
    # Pick the most recently modified file if multiple exist
    target_file = max(found_files, key=os.path.getmtime)
    print(f"Loading: {os.path.basename(target_file)}")
    
    if target_file.endswith('.csv'):
        return pd.read_csv(target_file)
    else:
        return pd.read_excel(target_file)

def generate_dashboard():
    print("--- Starting Dashboard Generation ---")

    # 1. LOAD DATASETS
    df_ord = load_file("order_Report_SA_ID")
    df_sales = load_file("order_sku_sales")
    df_lmd = load_file("iot_orderwise_rep")
    df_ota = load_file("OTA")
    df_pick = load_file("B2B_ORDER_pICK")
    df_soc = load_file("Migrated Societies Data")

    if df_ord is None:
        print("Critical Error: Primary Order Report not found.")
        return

    # 2. PROCESS ORDERS (Primary Metrics)
    df_ord['delivery_date'] = pd.to_datetime(df_ord['delivery_date']).dt.normalize()
    orders_agg = df_ord.groupby(['delivery_date', 'sa_name']).agg(
        unique_customers=('member_id', 'nunique'),
        total_orders=('order_id', 'nunique'),
        orders_delivered=('order_status', lambda x: x[x.isin(['complete', 'delivered'])].count()),
        sub_orders=('Type', lambda x: (x == 'Subscription').sum()),
        topup_orders=('Type', lambda x: (x == 'Topup').sum()),
        oos_cancellations=('cancellation_reason', lambda x: x.str.contains('OOS|Out of stock', case=False, na=False).sum()),
        cx_cancellations=('cancellation_reason', lambda x: x.str.contains('customer', case=False, na=False).sum())
    ).reset_index()

    # 3. PROCESS SALES
    if df_sales is not None:
        df_sales['delivery_date'] = pd.to_datetime(df_sales['delivery_date']).dt.normalize()
        sales_agg = df_sales.groupby(['delivery_date', 'sa_name']).agg(
            total_qty=('quantity', 'sum'),
            revenue=('total_sales', 'sum'),
            milk_qty=('Milk / NM', lambda x: df_sales.loc[x.index, 'quantity'][x == 'Milk'].sum())
        ).reset_index()
        orders_agg = pd.merge(orders_agg, sales_agg, on=['delivery_date', 'sa_name'], how='left')

    # 4. PROCESS LMD (OTD Metrics)
    if df_lmd is not None:
        df_lmd['dt'] = pd.to_datetime(df_lmd['order_delivered_time'], errors='coerce').dt.normalize()
        df_lmd['is_otd'] = (pd.to_datetime(df_lmd['order_delivered_time']).dt.time < pd.to_datetime('07:00:00').time())
        lmd_agg = df_lmd.groupby(['dt', 'sa_name']).agg(
            otd_perc=('is_otd', 'mean'),
            total_routes=('route_id', 'nunique')
        ).reset_index()
        orders_agg = pd.merge(orders_agg, lmd_agg, left_on=['delivery_date', 'sa_name'], right_on=['dt', 'sa_name'], how='left')

    # 5. PROCESS OTA (Truck Arrival)
    if df_ota is not None:
        # Handling multiple 'Store Name' columns in OTA report
        ota_store_col = 'Viapoint Name' if 'Viapoint Name' in df_ota.columns else 'Store Name'
        df_ota['Date'] = pd.to_datetime(df_ota['Date']).dt.normalize()
        df_ota['is_ota'] = pd.to_datetime(df_ota['Arrival Time'], format='%H:%M:%S', errors='coerce').dt.time <= pd.to_datetime('03:00:00').time()
        ota_agg = df_ota.groupby(['Date', ota_store_col])['is_ota'].any().reset_index()
        orders_agg = pd.merge(orders_agg, ota_agg, left_on=['delivery_date', 'sa_name'], right_on=['Date', ota_store_col], how='left')

    # 6. PROCESS PICKING
    if df_pick is not None:
        df_pick['DeliveryDAte'] = pd.to_datetime(df_pick['DeliveryDAte'], dayfirst=True, errors='coerce').dt.normalize()
        df_pick['on_time_pick'] = pd.to_datetime(df_pick['OPST_binned_time']).dt.time <= pd.to_datetime('04:00:00').time()
        pick_agg = df_pick.groupby(['DeliveryDAte', 'Serviceability_Area']).agg(
            picking_on_time=('on_time_pick', 'sum'),
            rod_qty=('order_status', lambda x: df_pick.loc[x.index, 'picked_quantity'][x == 'return-on-delivery'].sum())
        ).reset_index()
        orders_agg = pd.merge(orders_agg, pick_agg, left_on=['delivery_date', 'sa_name'], right_on=['DeliveryDAte', 'Serviceability_Area'], how='left')

    # 7. FINAL FORMATTING
    orders_agg.fillna(0, inplace=True)
    
    # Rename for the final "Day wise Format"
    final_rename = {
        'delivery_date': 'Date', 'sa_name': 'Store Name',
        'unique_customers': 'Total Ordered Customers (Unique)',
        'total_orders': 'Total Orders', 'orders_delivered': 'Orders Delivered',
        'sub_orders': 'Subscription Orders', 'topup_orders': 'Top-up Orders',
        'oos_cancellations': 'Undelivered Orders Due to OOS',
        'cx_cancellations': 'Cancelled Orders by Customer',
        'otd_perc': 'On-Time Delivery (Before 7:00 AM)',
        'revenue': 'Sale(₹)', 'total_routes': 'Total Routes',
        'is_ota': 'On Time Arrival (03:00 AM)',
        'picking_on_time': 'On Time Picking (Before 4 AM)',
        'rod_qty': 'ROD Quantity'
    }
    orders_agg.rename(columns=final_rename, inplace=True)

    # Filter to requested columns
    cols = [v for k, v in final_rename.items() if v in orders_agg.columns]
    final_df = orders_agg[cols]
    
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"--- Success! Dashboard saved as: {OUTPUT_FILE} ---")

if __name__ == "__main__":
    generate_dashboard()
