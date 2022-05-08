# one snapshot with incremental load and per day historical data from the snapshot table


set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.execution.reduce.enabled = true;
set hive.vectorized.execution.reduce.groupby.enabled = true;
set yarn.nodemanager.resource.memory-mb=8192;
set yarn.scheduler.maximum-allocation-mb=8192;
SET hive.tez.container.size=10240;
SET hive.tez.java.opts=-Xmx8192m;
set tez.runtime.io.sort.mb=4096;
set tez.runtime.unordered.output.buffer.size-mb=1024
set hive.optimize.sort.dynamic.partition=true

# --------update final table incrementally ----------#

#step 1- Delete and recreate a temp table
query1=" DROP TABLE IF EXISTS business_temp;
CREATE TABLE business_temp
STORED AS PARQUET
LOCATION 'gs://yelp_etl_bucket/temp_tables/business'
AS SELECT *
FROM yelp_dataset_etl.business_snapshot
LIMIT 0;"

#step2 - Copy data that not present in stage table but present in final table (i.e no changes) to temp table
query2="
INSERT INTO TABLE business_temp
SELECT f.* from
yelp_dataset_etl.business_snapshot f
LEFT JOIN
yelp_dataset_etl.business_dim_stage s
on f.business_id=s.business_id
AND f.category=s.category
WHERE s.business_id is null;
"

# step3-Copy all tha data from stage table that present in final table as well(i.e changed rows)
query3="
INSERT INTO TABLE business_temp
SELECT s.* from
yelp_dataset_etl.business_snapshot f
 JOIN
yelp_dataset_etl.business_dim_stage s
on f.business_id=s.business_id
AND f.category=s.category;
"

#step4 - Copy all data that not present in final but present in stage table(i.e new entries)
query4="
INSERT INTO TABLE business_temp
SELECT s.* from
yelp_dataset_etl.business_dim_stage s
LEFT JOIN
yelp_dataset_etl.business_snapshot f
on f.business_id=s.business_id
AND f.category=s.category
WHERE f.business_id is null;
"
#step5 - load the data to the final table from temp table

query5="
INSERT OVERWRITE TABLE yelp_dataset_etl.business_snapshot  select * from business_temp;
"

### snapshot table DDL
snapshot_table="CREATE TABLE IF NOT EXISTS yelp_dataset_etl.business_snapshot
              (
              business_id string,
              business_name string,
              category string,
              address string,
              city string,
              state string,
              zip_code string,
              latitude float,
              longitude float,
              star_rating float,
              review_count int,
              open_ind smallint,
              
              monday_open_time string,
              monday_close_time string,
              
              tuesday_open_time string,
              tuesday_close_time string,
              
              wednesday_open_time string,
              wednesday_close_time string,
              
              thursday_open_time string,
              thursday_close_time string,
              
              friday_open_time string,
              friday_close_time string,
              
              saturday_open_time string,
              saturday_close_time string,
              
              sunday_open_time string,
              sunday_close_time string,
              
              upd_ts timestamp
              )
              stored as parquet
              LOCATION
              'gs://yelp_etl_bucket/final_tables/business_snapshot'; "







#### ------------------------Historical data load -----------------------###
#historical table DDL

history_table="
CREATE TABLE IF NOT EXISTS yelp_dataset_etl.business_snapshot_history
              (
              business_id string,
              business_name string,
              category string,
              address string,
              city string,
              state string,
              zip_code string,
              latitude float,
              longitude float,
              star_rating float,
              review_count int,
              open_ind smallint,

              monday_open_time string,
              monday_close_time string,

              tuesday_open_time string,
              tuesday_close_time string,

              wednesday_open_time string,
              wednesday_close_time string,

              thursday_open_time string,
              thursday_close_time string,

              friday_open_time string,
              friday_close_time string,

              saturday_open_time string,
              saturday_close_time string,

              sunday_open_time string,
              sunday_close_time string,

              upd_ts timestamp
              )
              PARTITIONED BY (
              year int,month int,day int)
              stored as parquet
              LOCATION
              'gs://yelp_etl_bucket/historical_tables/business_snapshot';
"

## just load the current data to the static partition (every day or week)

load_history="
insert into table  yelp_dataset_etl.business_snapshot_history partition(year=2022,month=5,day=21)
select * from yelp_dataset_etl.business_snapshot;
"
## other way to copy data of current snapshot from gs bucket location to the appropriate partition and run
repair_table=" Msck repair table yelp_dataset_etl.business_snapshot_history;"




