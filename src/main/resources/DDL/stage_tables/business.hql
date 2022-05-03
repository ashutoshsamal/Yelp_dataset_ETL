DROP TABLE IF EXISTS yelp_dataset_etl.business_dim_stage;

CREATE TABLE IF NOT EXISTS yelp_dataset_etl.business_dim_stage
(
business_id string,
business_name string,
category string,
address string,
city string,
zip_code string,
latitude float,
longitude float,
star_rating float,
review_count int,
open_ind smallint,


monday_open_time timestamp,
monday_close_time timestamp,

tuesday_open_time timestamp,
tuesday_close_time timestamp,

wednesday_open_time timestamp,
wednesday_close_time timestamp,

thursday_open_time timestamp,
thursday_close_time timestamp,

friday_open_time timestamp,
friday_close_time timestamp,

saturday_open_time timestamp,
saturday_close_time timestamp,

sunday_open_time timestamp,
sunday_close_time timestamp

)
stored as parquet
LOCATION
'gs://yelp_etl_bucket/stage_tables/business';




