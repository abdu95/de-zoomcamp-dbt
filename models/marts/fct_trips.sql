WITH deduplicated AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (
      PARTITION BY 
        vendor_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        fare_amount,
        total_amount
      ORDER BY pickup_datetime
    ) AS row_num
  FROM {{ ref('int_trips_unioned') }}
)

SELECT 
  {{ dbt_utils.generate_surrogate_key([
    'vendor_id',
    'pickup_location_id', 
    'dropoff_location_id',
    'pickup_datetime'
  ]) }} AS trip_id,
  vendor_id,
  rate_code_id,
  pickup_location_id,
  dropoff_location_id,
  pickup_datetime,
  dropoff_datetime,
  store_and_fwd_flag,
  passenger_count,
  trip_distance,
  trip_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type,
  service_type
FROM deduplicated
WHERE row_num = 1