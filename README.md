 # DATA ENGINEERING ON YELP DATASET






# SUMMERY

This is an end to end data engineering project for YELP data set (https://www.yelp.com/dataset/documentation/main) that 
I build to get a hands-on experience on spark and hadoop ecosystem.I have used scala as a programing language , 
spark as compute engine and hive as warehouse and used data proc cluster in GCP to deploy my code. 

# DATASET

Yelp data set contains five types of data Business,review,user,checkin,tip.I took review and tips as fact table in my 
warehouse and other tables as their dimensions.

# ETL 
I tried to cover different ways to process data so followed different ways of ETL for all this tables as follows:-

## How we receive the data ?

Let's assume there is a firm which collecting this data for their analytical purpose and a json file is created as soon 
as there is any new additions  to any of the five data or if there is any changes in the existing business or user data 
(i.e any changes in star rating,categor,fan etc) or  and this json files are pushed to any kafka topic, and we're
dumping the data from kafka topic to the gs bucket for further process.


## Moving data from gcs bucket to hive stage table

###  BUSINESS
->I exploded the category column and chose business_id and category as my grain so we might have repeated business_ids 
  but combination of business id and category will be unique

->As json files are getting created as soon as there is some changes in the existing data there is chance of duplicate 
  records in the kafka topic if any business have multiple changes between two runs of our ETL.

->So I did deduplication of data after reading it in spark 
(/src/main/scala/yelpdata/hive/ingestion/gcs_to_stage/businessToStage.scala)

->After the ETL, I loaded the data into the stage hive table for incremental load of final table


## Moving data from stage to final table

### BUSINESS

-> As we have delta data(i.e All changes from the last run) in the stage table we can perform incremental load and
update the final table

-> I have maintained a final snapshot table to store the current state of all the records, and we update the records in 
this table incrementally.

-> Hive commands for this step : /src/main/scala/yelpdata/hive/ingestion/stage_to_final/business_stg_to_final_hive.sh

#### Preserving historical changes of the data

-> As we update the table in every run ,we are losing the historical data .So I have created a historical table to 
with same schema as the final table but with 3 nested partition(year,month,day)

-> We load this table every day/week from the snapshot table(i.e current state) to the appropriate partition.

-> Hive commands for this : /src/main/scala/yelpdata/hive/ingestion/stage_to_final/business_stg_to_final_hive.sh













