with 

source as (

    select * from {{ source('staging', 'fhv__2019') }}

),

renamed as (

    select
        int64_field_0 as row_num,
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid as pickup_locationid,
        dolocationid as dropoff_locationid,
        sr_flag,
        affiliated_base_number

    from source

)

select * from renamed
