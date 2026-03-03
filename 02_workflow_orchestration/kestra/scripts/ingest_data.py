import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click
import os
import requests
import sys

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
@click.option('--csv-file', default='yellow_tripdata_2021-01.csv', help='URL or Path to the CSV file')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, target_table, csv_file):

    # 1. Setup Koneksi
    conn_string = f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    engine = create_engine(conn_string)

    # 2. Penanganan URL & Pengecekan 404 (Graceful Exit)
    if csv_file.startswith('http'):
        print(f"Mengecek data di: {csv_file}")
        
        # HEAD request untuk cek ketersediaan tanpa download
        check = requests.head(csv_file)
        if check.status_code == 404:
            print(f"\n[SKIP] Data tidak tersedia (404). Melewati bulan ini secara aman.")
            return # Keluar dari skrip dengan sukses
            
        print("Data ditemukan! Memulai unduhan...")
        response = requests.get(csv_file, stream=True)
        local_file = "data_ingest_temp.csv.gz"
        with open(local_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        csv_file = local_file
    
    # 3. Definisi Tipe Data (Schema)
    dtype = {
        "VendorID": "Int64", "passenger_count": "Int64", "trip_distance": "float64",
        "RatecodeID": "Int64", "store_and_fwd_flag": "string", "PULocationID": "Int64",
        "DOLocationID": "Int64", "payment_type": "Int64", "fare_amount": "float64",
        "extra": "float64", "mta_tax": "float64", "tip_amount": "float64",
        "tolls_amount": "float64", "improvement_surcharge": "float64",
        "total_amount": "float64", "congestion_surcharge": "float64"
    }
    parse_dates = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

    # 4. Inisialisasi Iterator
    # Pandas secara otomatis akan mendeteksi kompresi .gz jika nama filenya tepat
    print(f"Mulai memproses file: {csv_file}")
    df_iter = pd.read_csv(
        csv_file,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=100000
    )

    # 5. Ingestion Loop (Logika Gabungan)
    first = True
    
    # file=sys.stdout agar log Kestra berwarna HIJAU (INFO), bukan KUNING (WARN)
    # total=14 adalah estimasi untuk file Jan 2021
    for df_chunk in tqdm(df_iter, total=14, desc="Ingesting", file=sys.stdout):
        if first:
            # Membuat tabel baru / Reset
            df_chunk.head(0).to_sql(name=target_table, con=engine, if_exists='replace')
            first = False
            print(f"\nTable '{target_table}' created/reset successfully.")

        # Memasukkan data per chunk
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')

    # Bersihkan file temp
    if os.path.exists(local_file):
        os.remove(local_file)

    print("\nProses Ingestion Selesai!")

if __name__ == '__main__':
    run()


