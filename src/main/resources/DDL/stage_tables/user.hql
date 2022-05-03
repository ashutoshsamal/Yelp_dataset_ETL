DROP TABLE IF EXISTS yelp_dataset_etl.user_dim_stage;

CREATE TABLE IF NOT EXISTS yelp_dataset_etl.user_dim_stage
(
 user_id string, 
 name string, 
 review_count int ,
 joined_date timestamp ,
 
 useful_votes_count int,
 funny_votes_count int,
 fans_count int,
 
 elite array<string>,
 
 average_stars_rating double ,
 
 hot_compliment_count  int,
 more_compliment_count int,
 profile_compliment_count int,
 cute_compliment_count int,
 list_compliment_count int,
 note_compliment_count int,
 plain_compliment_count int,
 cool_compliment_count int,
 funny_compliment_count int,
 writer_compliment_count int,
 photos_compliment_count int,
 
 upd_ts timestamp 

)
stored as parquet
LOCATION
'gs://yelp_etl_bucket/stage_tables/user';