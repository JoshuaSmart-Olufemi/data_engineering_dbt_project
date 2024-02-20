{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *
    , 'Green' as service_type
    from {{ ref('stg_staging__green_tripdata') }}
),

yellow_tripdata as (
    select *
    , 'Yellow' as service_type
    from {{ ref('stg_staging__yellow_tripdata') }}
),

union_data as (
    select * from green_tripdata
    union all
    select * from yellow_tripdata
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
        union_data.trip_id, 
        cast(union_data.vendorid as integer) as vendorid, 
        union_data.service_type,
        cast(union_data.ratecodeid as numeric) as ratecodeid, 
        cast(union_data.pickup_locationid as integer) as pickup_locationid, 
        pickup_zone.borough as pickup_borough, 
        pickup_zone.zone as pickup_zone, 
        cast(union_data.dropoff_locationid as integer) dropoff_locationid,
        dropoff_zone.borough as dropoff_borough, 
        dropoff_zone.zone as dropoff_zone,  
        cast(union_data.pickup_datetime as timestamp) as pickup_datetime, 
        cast(union_data.dropoff_datetime as timestamp) as dropoff_datetime, 
        union_data.store_and_fwd_flag, 
        cast(union_data.passenger_count as integer) as passenger_count, 
        cast(union_data.trip_distance as numeric) as trip_distance, 
        cast(union_data.trip_type as numeric) as trip_type, 
        cast(union_data.fare_amount as numeric) as fare_amount, 
        cast(union_data.extra as numeric) as extra, 
        cast(union_data.mta_tax as numeric) as mta_tax, 
        cast(union_data.tip_amount as numeric) as tip_amount, 
        cast(union_data.tolls_amount as numeric) as tolls_amount, 
        union_data.ehail_fee, 
        cast(union_data.improvement_surcharge as numeric) as improvement_surcharge, 
        cast(union_data.total_amount as numeric) as total_amount, 
        cast(union_data.payment_type as numeric) as payment_type, 
        union_data.payment_type_description


from union_data
inner join dim_zones as pickup_zone
on cast(union_data.pickup_locationid as integer) = cast(pickup_zone.locationid as integer)
inner join dim_zones as dropoff_zone
on cast(union_data.dropoff_locationid as integer) = cast(dropoff_zone.locationid as integer)

 -- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

limit 100

{% endif %}

