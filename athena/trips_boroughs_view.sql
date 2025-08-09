CREATE OR REPLACE VIEW nyc_taxi.v_trips_borough_hour AS
SELECT
  date_trunc('hour', tpep_pickup_datetime) AS pickup_hour,
  zpu.Borough AS pickup_borough,
  zdo.Borough AS dropoff_borough,
  trip_distance,
  passenger_count,
  fare_amount,
  tip_amount,
  total_amount
FROM nyc_taxi.yellow_curated y
LEFT JOIN nyc_taxi.taxi_zone_lookup zpu
  ON y.pulocationid = zpu.LocationID
LEFT JOIN nyc_taxi.taxi_zone_lookup zdo
  ON y.dolocationid = zdo.LocationID;
