{{ config(materialized='view') }}
with 

source as (

    select * 
    , row_number() over (partition by vendor_id, pickup_datetime) as rn
    from {{ source('staging', 'yellow_tripdata') }}
    where vendor_id is not null 

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime']) }} as trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        rate_code,
        store_and_fwd_flag,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        imp_surcharge,
        airport_fee,
        total_amount,
        pickup_location_id,
        dropoff_location_id,
        data_file_year,
        data_file_month,
         {{ get_payment_type_description('payment_type') }} as payment_type_description
    from source
    where rn = 1

)

select * from renamed
