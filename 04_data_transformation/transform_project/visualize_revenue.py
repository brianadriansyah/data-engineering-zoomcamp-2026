import duckdb
import plotly.express as px
import pandas as pd

# 1. Koneksi ke database
db_path = r'D:/Code-Project/Data-zoomcamp-2026/03_data_warehouse/my_data.db'

try:
    conn = duckdb.connect(db_path)
    
    # 2. Ambil data dari Mart Layer
    query = "SELECT * FROM dm_monthly_revenue ORDER BY revenue_month"
    df = conn.execute(query).df()
    
    # 3. Visualisasi Tren Pendapatan per Bulan per Borough
    fig = px.line(df, 
                  x="revenue_month", 
                  y="total_monthly_revenue", 
                  color="pickup_borough",
                  title="Tren Pendapatan Bulanan Taksi Kuning per Borough (2021)",
                  labels={"total_monthly_revenue": "Total Pendapatan ($)", "revenue_month": "Bulan"},
                  template="plotly_dark")
    
    print("Menampilkan grafik di browser...")
    fig.show()
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")