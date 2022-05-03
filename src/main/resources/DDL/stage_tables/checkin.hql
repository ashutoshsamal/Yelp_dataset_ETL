DROP TABLE IF EXISTS yelp_dataset_etl.checkin_stage;

CREATE TABLE IF NOT EXISTS yelp_dataset_etl.checkin_stage
(
business_table_pk string,
checkin_time_stamp timestamp,
cchekin_year int,
checkin_month int,
checkin_day int,
upd_ts timestamp
)
stored as parquet
LOCATION
'gs://yelp_etl_bucket/stage_tables/checkin';