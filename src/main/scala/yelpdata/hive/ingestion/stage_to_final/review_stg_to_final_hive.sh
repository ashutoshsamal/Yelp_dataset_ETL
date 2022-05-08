## Incremental load


## create final table in not exists


final_table="CREATE TABLE IF NOT EXISTS yelp_dataset_etl.reviews_final
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
                           'gs://yelp_etl_bucket/final_tables/review';"


## create temp table
temptable_query="
 CREATE TABLE temp_review AS
 select
 t2.review_id ,
 t2.user_id ,
 t2.business_id ,
 t2.stars,
 t2.review_text,
 t2.useful_votes_count,
 t2.funny_votes_count,
 t2.cool_votes_count,
 t2.review_timestamp,
 t2.review_month,
 t2.review_year,
 t2.review_day,
 t2.upd_ts
 from
 (select *,ROW_NUMBER() over(PARTITION BY review_id,review_timestamp ORDER BY review_timestamp DESC)
   from (SELECT * from yelp_dataset_etl.reviews_fact_stage UNION )
 
 )

"