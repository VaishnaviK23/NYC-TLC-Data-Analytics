CREATE OR REPLACE VIEW nyc_taxi.v_features_tip AS
SELECT
  CAST(date_format(tpep_pickup_datetime, '%H') AS integer) AS pickup_hour,
  date_format(tpep_pickup_datetime, '%a') AS dow,
  COALESCE(zpu.Borough, 'Unknown') AS pickup_borough,
  COALESCE(zdo.Borough, 'Unknown') AS dropoff_borough,
  passenger_count,
  trip_distance,
  payment_type,
  fare_amount,
  tip_amount  -- target for regression
FROM nyc_taxi.yellow_curated y
LEFT JOIN nyc_taxi.taxi_zone_lookup zpu
  ON y.pulocationid = zpu.LocationID
LEFT JOIN nyc_taxi.taxi_zone_lookup zdo
  ON y.dolocationid = zdo.LocationID;
