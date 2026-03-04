import duckdb
import os

# 1. Definisi Path (Gunakan Raw String 'r' agar Windows Path tidak error)
db_path = r'D:/Code-Project/Data-zoomcamp-2026/03_data_warehouse/my_data.db'
parquet_path = r'D:/Code-Project/Data-zoomcamp-2026/03_data_warehouse/data/yellow_tripdata_2021-07.parquet'

try:
    # 2. Koneksi ke Database
    conn = duckdb.connect(db_path)
    
    print("Sedang memproses perubahan objek...")

    # 3. Hapus VIEW yang sudah ada secara eksplisit
    # 'IF EXISTS' digunakan agar tidak error jika view ternyata sudah tidak ada
    conn.execute("DROP VIEW IF EXISTS yellow_taxi_trips")
    
    # 4. Buat TABEL fisik baru dari file Parquet
    conn.execute(f"CREATE TABLE yellow_taxi_trips AS SELECT * FROM '{parquet_path}'")
    
    print("--- Berhasil! ---")
    print(f"View lama telah dihapus dan diganti dengan Tabel fisik 'yellow_taxi_trips'.")
    print(f"Data diambil dari: {os.path.basename(parquet_path)}")
    
    conn.close()
except Exception as e:
    print(f"Terjadi kesalahan: {e}")