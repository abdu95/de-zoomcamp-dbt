with source as (
    select 
        dispatching_base_num,
        pickup_datetime,
        dropOff_datetime as dropoff_datetime ,
        PUlocationID AS pickup_location_id,
        DOlocationID as dropoff_location_id,
        SR_Flag as sr_flag,
        Affiliated_base_number as Affiliated_base_num 
    from {{ source('raw_data', 'fhv_tripdata_2019') }}
)

select * from source 
where dispatching_base_num is NOT NULL