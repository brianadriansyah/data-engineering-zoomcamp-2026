import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import os

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--csv-file', default='yellow_tripdata_2021-01.csv', help='Path to the CSV file')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, csv_file):

    # 1. Setup Koneksi
    # Menggunakan f-string untuk fleksibilitas parameter
    conn_string = f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    engine = create_engine(conn_string)

    # 2. Inteligensia Skema (Cek Kolom Terlebih Dahulu)
    # intip header file CSV-nya
    df_preview = pd.read_csv(csv_file, nrows=0)
    columns = df_preview.columns.tolist()

    # Inisialisasi variabel default
    dtype = None
    parse_dates = None

    # Logika deteksi: Jika ini file taksi (ada kolom tpep), gunakan skema ketat
    if "tpep_pickup_datetime" in columns:
        print("Deteksi: Data Yellow Taxi ditemukan. Menerapkan skema datetime...")
        dtype = {
            "VendorID": "Int64", "passenger_count": "Int64", "trip_distance": "float64",
            "RatecodeID": "Int64", "store_and_fwd_flag": "string", "PULocationID": "Int64",
            "DOLocationID": "Int64", "payment_type": "Int64", "fare_amount": "float64"
        }
        parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
    else:
        print("Deteksi: Data non-taksi (seperti Zones) ditemukan. Menggunakan inferensi otomatis...")

    # 3. Inisialisasi Iterator
    print(f"Mulai membaca file: {csv_file}")
    df_iter = pd.read_csv(
        csv_file,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=100000
    )

    # 4. Ingestion Loop
    first = True
    # Untuk file kecil seperti zones, total chunk hanya 1
    for df_chunk in tqdm(df_iter, desc=f"Ingesting {target_table}"):
        if first:
            # Membuat tabel baru / Reset
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace')
            first = False
            print(f"\nTable '{target_table}' created/reset successfully.")

        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')

    print(f"\nProses Ingestion '{target_table}' Selesai!")

if __name__ == '__main__':
    run()
