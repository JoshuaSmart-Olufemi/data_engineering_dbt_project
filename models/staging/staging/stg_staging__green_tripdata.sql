with 

source as (

    select * 
    , row_number() over (partition by vendor_id, pickup_datetime) as rn
    from {{ source('staging', 'green_tripdata') }}
    where vendor_id is not null

),

renamed as (

    select
        {{ dbt_utils.generate_surrogate_key(['vendor_id', 'pickup_datetime']) }} as trip_id,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        rate_code,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        airport_fee,
        total_amount,
        payment_type,
        distance_between_service,
        time_between_service,
        trip_type,
        imp_surcharge,
        pickup_location_id,
        dropoff_location_id,
        data_file_year,
        data_file_month
        , {{ get_payment_type_description('payment_type') }} as payment_type_description
    from source
    where rn = 1

)

select * from renamed
