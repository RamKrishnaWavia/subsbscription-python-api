import pandas as pd
import glob
import os

# --- CONFIGURATION ---
# If your files are in the same folder as the script, leave it as "."
# If they are in a subfolder like 'raw_data', change it to "./raw_data"
DATA_FOLDER = "." 
OUTPUT_FILE = "final_daily_dashboard_summary.csv"

def find_file(keyword):
    """Finds a file in the directory that contains a specific keyword."""
    files = glob.glob(os.path.join(DATA_FOLDER, "*.csv"))
    for f in files:
        if keyword.lower() in f.lower():
            return f
    return None

def process_dashboard():
    print("Starting Data Consolidation...")

    # 1. Map Files (Keyword Search)
    file_map = {
        "orders": find_file("order_Report_SA_ID"),
        "sales": find_file("order_sku_sales"),
        "lmd": find_file("iot-rate-card"),
        "ota": find_file("OTA"),
        "picking": find_file("B2B_ORDER_pICK"),
        "societies": find_file("Migrated Societies Data")
    }

    # Verify critical files exist
    if not file_map["orders"]:
        print("Error: Could not find Orders Report file in the folder.")
        return

    # 2. Load and Clean Orders (The Primary Source)
    df_ord = pd.read_csv(file_map["orders"])
    df_ord['delivery_date'] = pd.to_datetime(df_ord['delivery_date']).dt.normalize()
    
    # Aggregating Order Metrics
    orders_agg = df_ord.groupby(['delivery_date', 'sa_name']).agg(
        unique_customers=('member_id', 'nunique'),
        total_orders=('order_id', 'nunique'),
        orders_delivered=('order_status', lambda x: x[x.isin(['complete', 'delivered'])].count()),
        sub_orders=('Type', lambda x: (x == 'Subscription').sum()),
        topup_orders=('Type', lambda x: (x == 'Topup').sum()),
        undelivered_oos=('cancellation_reason', lambda x: x.str.contains('OOS|Out of stock', case=False, na=False).sum()),
        cancelled_by_cx=('cancellation_reason', lambda x: x.str.contains('customer', case=False, na=False).sum())
    ).reset_index()

    # 3. Load and Clean Sales
    if file_map["sales"]:
        df_sales = pd.read_csv(file_map["sales"])
        df_sales['delivery_date'] = pd.to_datetime(df_sales['delivery_date']).dt.normalize()
        sales_agg = df_sales.groupby(['delivery_date', 'sa_name']).agg(
            total_qty=('quantity', 'sum'),
            revenue=('total_sales', 'sum')
        ).reset_index()
        orders_agg = pd.merge(orders_agg, sales_agg, on=['delivery_date', 'sa_name'], how='left')

    # 4. Load LMD (OTD Metrics)
    if file_map["lmd"]:
        df_lmd = pd.read_csv(file_map["lmd"])
        df_lmd['dt'] = pd.to_datetime(df_lmd['order_delivered_time'], errors='coerce').dt.normalize()
        # OTD check: Delivered before 7 AM
        df_lmd['is_otd'] = (pd.to_datetime(df_lmd['order_delivered_time']).dt.time < pd.to_datetime('07:00:00').time())
        lmd_agg = df_lmd.groupby(['dt', 'sa_name']).agg(
            otd_perc=('is_otd', 'mean'),
            total_routes=('route_id', 'nunique'),
            cee_count=('cee_id', 'nunique')
        ).reset_index()
        orders_agg = pd.merge(orders_agg, lmd_agg, left_on=['delivery_date', 'sa_name'], right_on=['dt', 'sa_name'], how='left')

    # 5. Load Picking (Operational Timelines)
    if file_map["picking"]:
        df_pick = pd.read_csv(file_map["picking"])
        df_pick['DeliveryDAte'] = pd.to_datetime(df_pick['DeliveryDAte'], dayfirst=True).dt.normalize()
        # Picking check: Binned before 4 AM
        df_pick['on_time_pick'] = pd.to_datetime(df_pick['OPST_binned_time']).dt.time <= pd.to_datetime('04:00:00').time()
        pick_agg = df_pick.groupby(['DeliveryDAte', 'Serviceability_Area']).agg(
            picking_on_time=('on_time_pick', 'sum'),
            rod_qty=('order_status', lambda x: df_pick.loc[x.index, 'picked_quantity'][x == 'return-on-delivery'].sum())
        ).reset_index()
        orders_agg = pd.merge(orders_agg, pick_agg, left_on=['delivery_date', 'sa_name'], right_on=['DeliveryDAte', 'Serviceability_Area'], how='left')

    # 6. Final Formatting to match "Day wise Format"
    # Rename columns to match your exact dashboard header
    orders_agg.rename(columns={
        'delivery_date': 'Date',
        'sa_name': 'Store Name',
        'unique_customers': 'Total Ordered Customers (Unique)',
        'total_orders': 'Total Orders',
        'orders_delivered': 'Orders Delivered',
        'sub_orders': 'Subscription Orders',
        'topup_orders': 'Top-up Orders',
        'undelivered_oos': 'Undelivered Orders Due to OOS',
        'cancelled_by_cx': 'Cancelled Orders by Customer',
        'otd_perc': 'On-Time Delivery (Before 7:00 AM)',
        'revenue': 'Sale(₹)',
        'total_routes': 'Total Routes',
        'cee_count': 'CEE Count (Unique)',
        'picking_on_time': 'On Time Picking (Before 4 AM)',
        'rod_qty': 'ROD Quantity'
    }, inplace=True)

    # Fill empty values with 0
    orders_agg.fillna(0, inplace=True)
    
    # Keep only relevant columns
    cols_to_keep = ['Date', 'Store Name', 'Total Ordered Customers (Unique)', 'Total Orders', 
                    'Orders Delivered', 'Subscription Orders', 'Top-up Orders', 
                    'Cancelled Orders by Customer', 'Undelivered Orders Due to OOS', 
                    'Sale(₹)', 'On-Time Delivery (Before 7:00 AM)', 'Total Routes', 
                    'CEE Count (Unique)', 'On Time Picking (Before 4 AM)', 'ROD Quantity']
    
    final_df = orders_agg[cols_to_keep]
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"Done! File saved as {OUTPUT_FILE}")

if __name__ == "__main__":
    process_dashboard()
