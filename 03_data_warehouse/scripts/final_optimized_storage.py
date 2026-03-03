import duckdb

con = duckdb.connect()

# 1. Path sumber dan target
input_parquet = 'data/yellow_tripdata_2021-07.parquet'
output_dir = 'data/optimized_taxi_data'

print("Memulai optimasi: Partitioning by VendorID + Clustering by Pickup Datetime...")

# 2. Proses pembuatan data ter-cluster
# Mengurutkan data berdasarkan pickup_datetime sebelum dipartisi

con.execute(f"""
    COPY (
        SELECT * FROM '{input_parquet}'
        ORDER BY tpep_pickup_datetime
    )
    TO '{output_dir}'
    (FORMAT PARQUET, PARTITION_BY (VendorID), OVERWRITE_OR_IGNORE);
""")

print(f"Selesai! Data optimal tersimpan di {output_dir}")

con.close()