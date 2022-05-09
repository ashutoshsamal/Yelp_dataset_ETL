DROP TABLE IF EXISTS yelp_dataset_etl.business_dim_incremental_load;

CREATE TABLE IF NOT EXISTS yelp_dataset_etl.business_dim_incremental_load
(

business_table_pk string,
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

monday_start_time timestamp,
tuesday_start_time timestamp,
wednesday_start_time timestamp,
thursday_start_time timestamp,
friday_start_time timestamp,
saturday_start_time timestamp,
sunday_start_time timestamp,

monday_end_time timestamp,
tuesday_end_time timestamp,
wednesday_end_time timestamp,
thursday_end_time timestamp,
friday_end_time timestamp,
saturday_end_time timestamp,
sunday_end_time timestamp,

created_ts timestamp,
upd_ts timestamp
);




// Incremental load