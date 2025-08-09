CREATE EXTERNAL TABLE taxi_zone_lookup (
  LocationID int,
  Borough string,
  Zone string,
  service_zone string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://genai-taxi-raw-poc/ref/'
TBLPROPERTIES ('skip.header.line.count'='1');
