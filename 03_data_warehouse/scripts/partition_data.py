import duckdb
import os

con = duckdb.connect()

# 1. Menentukan path input dan output
input_parquet = 'data/yellow_tripdata_2021-07.parquet'
output_dir = 'data/partitioned_taxi_data'

print(f"Memulai proses partitioning ke {output_dir}...")

# 2. Menjalankan perintah ekspor dengan PARTITION_BY
# Membagi berdasarkan VendorID sebagai contoh awal
con.execute(f"""
    COPY (SELECT * FROM '{input_parquet}')
    TO '{output_dir}'
    (FORMAT PARQUET, PARTITION_BY (VendorID), OVERWRITE_OR_IGNORE);
""")

print("Proses partitioning selesai")
con.close()