import duckdb
import time

con = duckdb.connect()

# 1. Template SQL yang bersih
# Kita biarkan {path} tanpa tanda kutip di sini, karena kita akan 
# memasukkan string yang sudah dibungkus tanda kutip dari fungsi.
query_template = """
SELECT count(*), AVG(total_amount)
FROM {path}
WHERE VendorID = 1 
  AND tpep_pickup_datetime BETWEEN '2021-07-01' AND '2021-07-03';
"""

def run_test(name, path, is_hive=False):
    print(f"--- Mengetes: {name} ---")
    
    # Bungkus path dengan tanda kutip tunggal yang benar
    if is_hive:
        actual_path = f"read_parquet('{path}', hive_partitioning=1)"
    else:
        actual_path = f"'{path}'"
    
    sql = query_template.format(path=actual_path)
    
    times = []
    for i in range(5):
        start_time = time.time()
        con.execute(sql).fetchone()
        end_time = time.time()
        times.append(end_time - start_time)
    
    avg_time = sum(times) / len(times)
    print(f"Rata-rata waktu: {avg_time:.5f} detik\n")
    return avg_time

# 2. Jalankan Tes
raw_file = 'data/yellow_tripdata_2021-07.parquet'
optimized_folder = 'data/optimized_taxi_data/**/*.parquet'

time_raw = run_test("Single Raw File (66MB)", raw_file)
time_opt = run_test("Partitioned + Clustered Folder", optimized_folder, is_hive=True)

if time_raw > 0:
    improvement = ((time_raw - time_opt) / time_raw) * 100
    print(f"KESIMPULAN: Data yang dioptimasi {improvement:.2f}% lebih cepat!")