DROP TABLE IF EXISTS yelp_dataset_etl.tip_fact;

CREATE TABLE IF NOT EXISTS yelp_dataset_etl.tip_fact
(
tip_table_pk string,
user_table_pk string,
business_table_pk string,
tip_data date,
tip_text string,
compliment_count integer

created_ts timestamp,
upd_ts timestamp



)