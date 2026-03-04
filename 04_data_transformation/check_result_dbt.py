# import duckdb
# import pandas as pd

# # 1. Konfigurasi Path
# # Menggunakan absolute path agar tidak ada masalah 'file not found'
# db_path = r'D:/Code-Project/Data-zoomcamp-2026/03_data_warehouse/my_data.db'

# def check_results():
#     try:
#         # Koneksi ke database
#         conn = duckdb.connect(db_path)
        
#         print("--- Memulai Pengecekan Hasil dbt ---")
        
#         # A. Cek daftar tabel/view yang ada di schema main
#         tables = conn.execute("SHOW TABLES").df()
#         print("\n[1] Daftar Tabel/View di Database:")
#         print(tables)
        
#         if 'stg_yellow_tripdata' not in tables['name'].values:
#             print("\nError: View 'stg_yellow_tripdata' tidak ditemukan!")
#             return

#         # B. Cek Sample Data dan Tipe Data
#         # Ini untuk memastikan casting (timestamp, integer) berhasil
#         df_sample = conn.execute("SELECT * FROM stg_yellow_tripdata LIMIT 5").df()
#         print("\n[2] Sample Data (5 baris pertama):")
#         print(df_sample[['trip_id', 'vendorid', 'pickup_datetime', 'pickup_locationid', 'total_amount']])
        
#         print("\n[3] Info Tipe Data Kolom (Cek Casting):")
#         print(df_sample.dtypes)

#         # C. Analisis Kritis: Cek Deduplikasi
#         # Membandingkan baris di tabel asal vs view staging
#         count_raw = conn.execute("SELECT COUNT(*) FROM yellow_taxi_trips").fetchone()[0]
#         count_stg = conn.execute("SELECT COUNT(*) FROM stg_yellow_tripdata").fetchone()[0]
        
#         print(f"\n[4] Analisis Jumlah Baris:")
#         print(f"Total baris di Raw Data   : {count_raw}")
#         print(f"Total baris di Staging    : {count_stg}")
#         print(f"Jumlah baris ter-filter/duplikat: {count_raw - count_stg}")

#         conn.close()
#         print("\n--- Pengecekan Selesai ---")

#     except Exception as e:
#         print(f"Terjadi kesalahan saat pengecekan: {e}")

# if __name__ == "__main__":
#     check_results()



# # Tambahkan baris ini di script Python Anda atau jalankan query ini
# import duckdb
# conn = duckdb.connect(r'D:/Code-Project/Data-zoomcamp-2026/03_data_warehouse/my_data.db')
# print(conn.execute("SELECT trip_id, pickup_borough, pickup_zone, total_amount FROM fact_trips LIMIT 5").df())


import duckdb
conn = duckdb.connect(r'D:/Code-Project/Data-zoomcamp-2026/03_data_warehouse/my_data.db')

# Query untuk mencari data yang menyebabkan test total_amount gagal
query = """
SELECT * FROM fact_trips 
WHERE total_amount < 0 
LIMIT 5
"""
print(conn.execute(query).df())