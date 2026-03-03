# Check Partition
import duckdb

query = """
    SELECT * FROM 'data/partitioned_taxi_data/**/*.parquet'
    LIMIT 5
    """

print(duckdb.query(query).df())