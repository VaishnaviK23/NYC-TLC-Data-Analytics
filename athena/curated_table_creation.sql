CREATE TABLE nyc_taxi.yellow_curated
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://genai-taxi-curated-poc/yellow/',
  partitioned_by = ARRAY['year', 'month']
) AS
SELECT
  -- Core columns (present in Yellow 2024 Parquet)
  vendorid,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  ratecodeid,
  store_and_fwd_flag,
  pulocationid,
  dolocationid,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,

  -- Partitions derived from pickup time
  CAST(date_format(tpep_pickup_datetime, '%Y') AS integer) AS year,
  CAST(date_format(tpep_pickup_datetime, '%m') AS integer) AS month
FROM "AwsDataCatalog"."nyc_taxi"."yellow_raw_20242024";
