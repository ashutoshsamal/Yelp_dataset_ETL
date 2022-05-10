


DROP TABLE IF EXISTS yelp_dataset_etl.reviews_fact;

CREATE TABLE IF NOT EXISTS yelp_dataset_etl.reviews_fact
(

review_table_pk string,
user_table_pk string,
business_table_pk string,
stars smallint,
review_date Date,
review_text string,
useful_votes_count int,
funny_votes_count int,
cool_votes_count int,

created_ts timestamp,
upd_ts timestamp

)