{{ config(materialized='table') }}

with yellow_tripdata as (
    select * from {{ ref('stg_yellow_tripdata') }}
),
dim_zones as (
    select * from {{ ref('taxi_zone_lookup') }}
    where borough != 'Unknown'
)
select
    yellow_tripdata.trip_id,
    yellow_tripdata.vendorid,
    yellow_tripdata.pickup_datetime,
    -- join dengan data zona untuk mendapatkan wilayah
    pickup_zone.borough as pickup_borough,
    pickup_zone.zone as pickup_zone,
    yellow_tripdata.dropoff_datetime,
    dropoff_zone.borough as dropoff_borough,
    dropoff_zone.zone as dropoff_zone,
    yellow_tripdata.passenger_count,
    yellow_tripdata.trip_distance,
    yellow_tripdata.fare_amount,
    yellow_tripdata.total_amount
from yellow_tripdata
inner join dim_zones as pickup_zone
    on yellow_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
    on yellow_tripdata.dropoff_locationid = dropoff_zone.locationid

    