import pandas as pd
import numpy as np

def generate_dashboard_data():
    # 1. LOAD DATA (Assuming CSV format from the provided files)
    orders = pd.read_csv("order_Report_SA_ID_BB2.0.csv")
    sku_sales = pd.read_csv("order_sku_sales_bb2_report.xlsx")
    iot = pd.read_csv("iot-rate-card-iot_orderwise_rep.csv")
    ota = pd.read_csv("OTA.csv")
    b2b_pick = pd.read_csv("B2B_ORDER_pICK.csv")
    societies = pd.read_csv("Migrated Societies Data.csv")

    # 2. DATE NORMALIZATION
    # Handling different date formats common in these reports
    orders['delivery_date'] = pd.to_datetime(orders['delivery_date']).dt.normalize()
    sku_sales['delivery_date'] = pd.to_datetime(sku_sales['delivery_date']).dt.normalize()
    iot['order_delivered_time_dt'] = pd.to_datetime(iot['order_delivered_time'], errors='coerce').dt.normalize()
    ota['Date'] = pd.to_datetime(ota['Date']).dt.normalize()
    b2b_pick['DeliveryDAte'] = pd.to_datetime(b2b_pick['DeliveryDAte'], dayfirst=True).dt.normalize()

    # 3. AGGREGATE METRICS BY STORE & DATE
    
    # Store-Level Society Count (Static reference)
    store_societies = societies[societies['Migration Status'] == 'Migrated'].groupby('SA Name').size().reset_index(name='societies_migrated')

    # Order Volume & Fulfillment Aggregation
    orders_agg = orders.groupby(['delivery_date', 'sa_name']).agg(
        total_customers=('member_id', 'nunique'),
        total_orders=('order_id', 'nunique'),
        delivered_orders=('order_status', lambda x: x[x.isin(['complete', 'delivered'])].count()),
        subscription_orders=('Type', lambda x: (x == 'Subscription').sum()),
        topup_orders=('Type', lambda x: (x == 'Topup').sum()),
        undelivered_oos=('cancellation_reason', lambda x: x.str.contains('OOS|Out of stock|no products', case=False, na=False).sum()),
        cancelled_by_cx=('cancellation_reason', lambda x: x.str.contains('customer', case=False, na=False).sum())
    ).reset_index()

    # Sales & Quantity Aggregation
    sales_agg = sku_sales.groupby(['delivery_date', 'sa_name']).agg(
        total_qty=('quantity', 'sum'),
        milk_qty=('Milk / NM', lambda x: sku_sales.loc[x.index, 'quantity'][x == 'Milk'].sum()),
        non_milk_qty=('Milk / NM', lambda x: sku_sales.loc[x.index, 'quantity'][x == 'Non-Milk'].sum()),
        revenue=('total_sales', 'sum')
    ).reset_index()

    # LMD & OTD (On-Time Delivery) Calculation
    # Logic: Delivered before 07:00:00 AM
    iot['is_otd'] = (pd.to_datetime(iot['order_delivered_time']).dt.time < pd.to_datetime('07:00:00').time()) & (iot['order_status'] == 'complete')
    otd_agg = iot.groupby(['order_delivered_time_dt', 'sa_name']).agg(
        otd_percentage=('is_otd', 'mean'),
        active_routes=('route_id', 'nunique'),
        active_cee=('cee_id', 'nunique')
    ).reset_index()

    # OTA (On-Time Arrival of Trucks)
    # Logic: Truck Arrival <= 03:00:00 AM
    ota['is_ota'] = pd.to_datetime(ota['Arrival Time'], format='%H:%M:%S', errors='coerce').dt.time <= pd.to_datetime('03:00:00').time()
    ota_agg = ota.groupby(['Date', 'Viapoint Name'])['is_ota'].any().reset_index()

    # Picking Performance (B2B Pick)
    # Logic: Binned before 04:00:00 AM
    b2b_pick['picking_on_time'] = pd.to_datetime(b2b_pick['OPST_binned_time']).dt.time <= pd.to_datetime('04:00:00').time()
    pick_agg = b2b_pick.groupby(['DeliveryDAte', 'Serviceability_Area']).agg(
        picking_ontime_count=('picking_on_time', 'sum'),
        rod_quantity=('order_status', lambda x: b2b_pick.loc[x.index, 'picked_quantity'][x == 'return-on-delivery'].sum())
    ).reset_index()

    # 4. FINAL MERGE (The "Data Join")
    # Base: Orders + Sales
    final = pd.merge(orders_agg, sales_agg, on=['delivery_date', 'sa_name'], how='left')

    # Join LMD/OTD
    final = pd.merge(final, otd_agg, left_on=['delivery_date', 'sa_name'], right_on=['order_delivered_time_dt', 'sa_name'], how='left')

    # Join Picking
    final = pd.merge(final, pick_agg, left_on=['delivery_date', 'sa_name'], right_on=['DeliveryDAte', 'Serviceability_Area'], how='left')

    # Join Societies
    final = pd.merge(final, store_societies, left_on='sa_name', right_on='SA Name', how='left')

    # 5. CLEANUP & EXPORT
    final.drop(columns=['order_delivered_time_dt', 'DeliveryDAte', 'Serviceability_Area', 'SA Name'], inplace=True)
    final.rename(columns={'delivery_date': 'Date', 'sa_name': 'Store'}, inplace=True)
    final.fillna(0, inplace=True)

    final.to_csv("consolidated_daily_dashboard.csv", index=False)
    print("Success: consolidated_daily_dashboard.csv has been created.")

if __name__ == "__main__":
    generate_dashboard_data()
