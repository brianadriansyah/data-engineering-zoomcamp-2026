import duckdb

# 1. Inisialisasi koneksi DuckDB dengan mode in-memory untuk proses extract saja
con = duckdb.connect()

# 2. Instal dan muat extraksi postgres

con.execute("INSTALL postgres;")
con.execute("LOAD postgres;")

print("Ekstensi Postgres berhasil dimuat")

# 3. String koneksi ke PostgreSQL (sesuai dengan kredensial Modul 1)
conn_str = "dbname=ny_taxi user=root password=root host=127.0.0.1 port=5432"

# 4. Melakukan Attach Database
con.execute(f"ATTACH '{conn_str}' AS postgres_db (TYPE POSTGRES);")

print("Berhasil terhubung (ATTACH) ke PostgreSQL!")

# 5. Verifikasi Data dengan menghitung jumlah baris di tabel yellow_taxi_data
row_count = con.execute("SELECT count(*) FROM postgres_db.yellow_taxi_data").fetchone()[0]

print(f"Koneksi sukses! Ditemukan {row_count} baris data di PostgreSQL.")

# 6. Menampilkan 5 data teratas
print("Menampilkan 5 data teratas:")
print(con.execute("SELECT * FROM postgres_db.yellow_taxi_data LIMIT 5").df())

# 7. Ekstraksi data ke parquet
output_path = 'data/yellow_tripdata_2021-07.parquet'

print(f"Sedang mengekspor data ke {output_path}...")

# Perintah SQL untuk menyalin data
con.execute(f"""
    COPY (SELECT * FROM postgres_db.yellow_taxi_data)
    TO '{output_path}'
    (FORMAT PARQUET);
""")

print("Ekspor selesai! File Parquet telah dibuat.")

# 8. Tutup koneksi DuckDB
con.close()

print("Koneksi DuckDB berhasil ditutup.")
