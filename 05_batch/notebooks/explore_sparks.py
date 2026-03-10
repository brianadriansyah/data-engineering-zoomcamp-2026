import pyspark
from pyspark.sql import SparkSession

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test_exploration') \
    .getOrCreate()

# Membaca data dari file parquet
df = spark.read.parquet('../data/pq/yellow_tripdata_2021-07.parquet')

# Mengubah nama kolom agar konsisten
df = df.withColumnRenamed('VendorID', 'vendor_id') \
       .withColumnRenamed('RatecodeID', 'ratecode_id')

# Menampilkan schema data
df.printSchema()
df.show(5)

# Transformasi Sederhana (Filter dan Select)
# Cari perjalanan yang jaraknya lebih dari 5 mil
long_trips = df.select('vendor_id', 'tpep_pickup_datetime', 'trip_distance') \
                .filter(df.trip_distance > 5)

print(f"Jumlah perjalanan jauh: {long_trips.count()}")

# # Menutup Session
spark.stop()