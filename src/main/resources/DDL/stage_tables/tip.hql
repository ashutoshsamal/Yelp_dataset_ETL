DROP TABLE IF EXISTS yelp_dataset_etl.tip_fact_stage;

CREATE  TABLE IF NOT EXISTS yelp_dataset_etl.tip_fact_stage
(
business_id string,
user_id string,
compliment_count integer,
tip_timestamp timestamp,
text string,
upd_ts timestamp
)
stored as parquet
LOCATION
'gs://yelp_etl_bucket/stage_tables/tip';