DROP TABLE IF EXISTS yelp_dataset_etl.user_dim_incremental_load;

CREATE TABLE IF NOT EXISTS yelp_dataset_etl.user_dim_incremental_load
(
user_table_pk string,
user_name string,
reviews_count integer,
joined_date date,
useful_votes_count integer,
funny_votes_count integer,
cool_votes_count integer,
fans_count integer,
average_stars_rating integer,

hot_compliment_count integer,
more_compliment_count integer,
profile_compliment_count integer,
cute_compliment_count integer,
list_compliment_count integer,
note_compliment_count integer,
plain_compliment_count integer,
cool_compliment_count integer,
funny_compliment_count integer,
write_compliment_count integer,
photo_compliment_count integer,



created_ts timestamp,
upd_ts timestamp


)