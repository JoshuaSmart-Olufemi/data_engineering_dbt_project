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
        union_data.vendorid, 
        union_data.service_type,
        union_data.ratecodeid, 
        union_data.pickup_locationid, 
        pickup_zone.borough as pickup_borough, 
        pickup_zone.zone as pickup_zone, 
        union_data.dropoff_locationid,
        dropoff_zone.borough as dropoff_borough, 
        dropoff_zone.zone as dropoff_zone,  
        union_data.pickup_datetime, 
        union_data.dropoff_datetime, 
        union_data.store_and_fwd_flag, 
        union_data.passenger_count, 
        union_data.trip_distance, 
        union_data.trip_type, 
        union_data.fare_amount, 
        union_data.extra, 
        union_data.mta_tax, 
        union_data.tip_amount, 
        union_data.tolls_amount, 
        union_data.ehail_fee, 
        union_data.improvement_surcharge, 
        union_data.total_amount, 
        union_data.payment_type, 
        union_data.payment_type_description


from union_data
inner join dim_zones as pickup_zone
on cast(union_data.pickup_locationid as integer) = cast(pickup_zone.locationid as integer)
inner join dim_zones as dropoff_zone
on cast(union_data.pickup_locationid as integer) = cast(dropoff_zone.locationid as integer)

 -- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

limit 100

{% endif %}

