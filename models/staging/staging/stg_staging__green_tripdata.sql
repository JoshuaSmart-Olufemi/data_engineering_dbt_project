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
        vendor_id as vendorid,
        rate_code as ratecodeid,
        pickup_location_id as  pickup_locationid,
        dropoff_location_id as dropoff_locationid,

        -- timestamps
        pickup_datetime as pickup_datetime,
        dropoff_datetime as dropoff_datetime,
        
        -- trip info
        store_and_fwd_flag,
        passenger_count as passenger_count,
        trip_distance as trip_distance,
        cast(trip_type as numeric) as trip_type,
        
        -- payment info
        fare_amount as fare_amount,
        extra as extra,
        mta_tax as mta_tax,
        tip_amount as tip_amount,
        tolls_amount as tolls_amount,
        ehail_fee as ehail_fee,
        imp_surcharge as improvement_surcharge,
        total_amount as total_amount,
        payment_type as payment_type,
        {{ get_payment_type_description('payment_type') }} as payment_type_description
    from source
    where rn = 1


    -- dbt build --m <model.sql> --var 'is_test_run: false'
    {% if var('is_test_run', default=true) %}

    limit 100

    {% endif %}

)

select * from renamed
