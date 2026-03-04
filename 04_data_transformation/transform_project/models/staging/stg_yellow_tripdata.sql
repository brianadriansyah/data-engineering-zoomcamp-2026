{{ config(materialized='view') }}

with tripdata as (
    select *,
      row_number() over(partition by vendorid, tpep_pickup_datetime) as rn
  from {{ source('staging','raw_yellow_taxi') }}
  where
    vendorid is not null
    and total_amount >= 0
    and passenger_count > 0
)

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as trip_id,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,

    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    cast(fare_amount as numeric) as fare_amount,
    cast(total_amount as numeric) as total_amount

from tripdata
where rn = 1
