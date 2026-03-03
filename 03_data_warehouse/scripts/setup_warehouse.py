import duckdb

# Menghubungkan ke DuckDB
con = duckdb.connect('main.db')

print("Berhasil terhubung ke main.db")

# Membuat 'External Table' menggunakan VIEW
con.execute("""
    CREATE OR REPLACE VIEW yellow_taxi_trips AS
    SELECT * FROM 'data/yellow_tripdata_2021-07.parquet';
""")

print("View 'yellow_taxi_trips' berhasil dibuat.")

# Query Analitis Pertama: rata-rata tarif berdasarkan jumlah penumpang

print("\nMenjalankan Analisis: rata-rata tarif per jumlah penumpang...")

result = con.execute("""
    SELECT 
        passenger_count, 
        ROUND(AVG(total_amount), 2) AS avg_fare,
        COUNT(*) AS total_trips
    FROM yellow_taxi_trips
    GROUP BY passenger_count
    ORDER BY passenger_count;
""").df()

print(result)

con.close()