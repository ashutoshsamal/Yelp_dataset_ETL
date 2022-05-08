# one snapshot with incremnetal load and per day historical data


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

# create final table
snapshot_table="CREATE TABLE IF NOT EXISTS yelp_dataset_etl.business_snapshot
              (
              business_id string,
              business_name string,
              category string,
              address string,
              state string,
              city string,

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

echo $snapshot_table

insert overwrite table yelp_dataset_etl.business_snapshot  select

                business_id,
               business_name,
               category,
               address,
               state,
              city,
               zip_code,
               latitude,
               longitude,
               star_rating,
               review_count,
               open_ind,

               monday_open_time,
               monday_close_time,

               tuesday_open_time,
               tuesday_close_time,

               wednesday_open_time,
               wednesday_close_time,

               thursday_open_time,
               thursday_close_time,

               friday_open_time,
               friday_close_time,

               saturday_open_time,
               saturday_close_time,

               sunday_open_time,
               sunday_close_time,
               upd_ts



      from yelp_dataset_etl.business_dim_stage;



