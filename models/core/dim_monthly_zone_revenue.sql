{{  config(materialized='table') }}

with trips_data as (
    select * from {{ ref('fct_trips') }}
),

final as (
    select
    pickup_zone as revenue_zone
    , date_trunc(pickup_datetime, month) as revenue_month
    , service_type

    , sum(fare_amount) as revenue_monthly_fare
    , sum(extra) as revenue_monthly_extra
    , sum(mta_tax) as revenue_monthly_mta_tax
    , sum(tip_amount) as revenue_monthly_tip_amount
    , sum(tolls_amount) as revenue_monthly_tolls_amount
    , sum(ehail_fee) as revenue_monthly_ehail_fee
    , sum(improvement_surcharge) as revenue_monthly_improvement_surcharge
    , sum(total_amount) as revenue_monthly_total_amount

    , count(trip_id) as total_monthly_trips
    , avg(passenger_count) as avg_monthly_passenger_count

    from trips_data
    group by 1,2,3


)

select * from final 
