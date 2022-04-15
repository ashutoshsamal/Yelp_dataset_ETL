DROP TABLE IF EXISTS yelp_dataset_etl.tip_fact_stage;

CREATE TABLE IF NOT EXISTS yelp_dataset_etl.tip_fact_stage
(
tip_table_pk string,
user_table_pk string,
business_table_pk string,
tip_data date,
tip_text string,
compliment_count integer





)