DROP TABLE IF EXISTS yelp_dataset_etl.reviews_fact_stage;

CREATE TABLE IF NOT EXISTS yelp_dataset_etl.reviews_fact_stage
(

review_id string,
user_id string,
business_id string,
stars float,
review_text string,
useful_votes_count int,
funny_votes_count int,
cool_votes_count int,
review_timestamp timestamp,
review_month integer,
review_year integer,
review_day integer,
upd_ts timestamp
)
stored as parquet
LOCATION
'gs://yelp_etl_bucket/stage_tables/review';