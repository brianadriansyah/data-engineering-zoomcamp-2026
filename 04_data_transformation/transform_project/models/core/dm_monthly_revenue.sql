{{ config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fact_trips')}}
)
select
    -- Truncate tanggal ke bulan
    date_trunc('month', pickup_datetime) as revenue_month,
    pickup_borough,
    -- metrik bisnis
    count(trip_id) as total_monthly_trips,
    sum(fare_amount) as total_monthly_fare,
    sum(total_amount) as total_monthly_revenue,
    avg(total_amount) as avg_revenue_per_trip

from trips_data
group by 1,2
