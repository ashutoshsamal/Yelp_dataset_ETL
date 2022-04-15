DROP TABLE IF EXISTS yelp_dataset_etl.chechin_stage;

CREATE TABLE IF NOT EXISTS yelp_dataset_etl.chechin_stage
(
business_table_pk string,
checkin_date date,
checkin_time timestamp


)