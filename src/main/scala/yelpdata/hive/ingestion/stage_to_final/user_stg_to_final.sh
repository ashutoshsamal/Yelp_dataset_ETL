#one snapshot and one scd2


#scd_start_date TIMESTAMP, -- start date and time
 #  scd_end_date TIMESTAMP, -- end date and time (9999-12-31 23:59:59 by default)
 #  scd_active int, -- whether it's the latest version or not

#-----Intial load--------#

#create final table
create_final="
CREATE TABLE IF NOT EXISTS yelp_dataset_etl.user_dim_final
(
 user_id string,
 name string,
 review_count int ,
 joined_date timestamp ,

 useful_votes_count int,
 funny_votes_count int,
 fans_count int,

 elite array<string>,

 average_stars_rating double ,

 hot_compliment_count  int,
 more_compliment_count int,
 profile_compliment_count int,
 cute_compliment_count int,
 list_compliment_count int,
 note_compliment_count int,
 plain_compliment_count int,
 cool_compliment_count int,
 funny_compliment_count int,
 writer_compliment_count int,
 photos_compliment_count int,

 upd_ts timestamp,
 scd_start_date TIMESTAMP,
 scd_end_date TIMESTAMP,
 scd_active int


)
stored as parquet
LOCATION
'gs://yelp_etl_bucket/final_tables/user';

"

# INSERT stage data to final table
insert_query_intial="
INSERT INTO yelp_dataset_etl.user_dim_final
select *,
from_unixtime(unix_timestamp()),
null,
1
from
yelp_dataset_etl.user_dim_stage;
"





#-----------SCD2---------#

temp_table="DROP TABLE IF EXISTS user_temp;
CREATE TABLE user_temp
STORED AS PARQUET
LOCATION 'gs://yelp_etl_bucket/temp_tables/user'
AS SELECT *
FROM yelp_dataset_etl.user_dim_final
LIMIT 0;"

#Copy all the records from the production table that don't exist in the staging table
query1=" INSERT INTO TABLE user_temp
SELECT f.* from
yelp_dataset_etl.user_dim_final f
LEFT JOIN
yelp_dataset_etl.user_dim_stage s
on f.user_id=s.user_id
WHERE s.user_id is null;
"

#Copy all inactive (historical) records from the production table

query2="INSERT INTO TABLE user_temp
select f.* from
yelp_dataset_etl.user_dim_final f
JOIN
yelp_dataset_etl.user_dim_stage s
on f.user_id=s.user_id
AND f.scd_active = 0;
"

#Insert new inactive (which was currently active in production) versions of records from the production table which have SCD Type 2 changes
query3="INSERT INTO TABLE user_temp
select 
f.user_id ,
f.name ,
f.review_count  ,
f.joined_date,

f.useful_votes_count ,
f.funny_votes_count ,
f.fans_count ,

f.elite ,

f.average_stars_rating ,

f.hot_compliment_count  ,
f.more_compliment_count ,
f.profile_compliment_count ,
f.cute_compliment_count ,
f.list_compliment_count ,
f.note_compliment_count ,
f.plain_compliment_count ,
f.cool_compliment_count ,
f.funny_compliment_count ,
f.writer_compliment_count ,
f.photos_compliment_count ,
current_timestamp(),
f.scd_start_date ,
current_timestamp(),
0
from
yelp_dataset_etl.user_dim_final f
JOIN
yelp_dataset_etl.user_dim_stage s
on f.user_id=s.user_id
AND f.scd_active = 1;

"

#Insert new active versions of records from the stage table which have SCD Type 2 changes
query4="INSERT INTO TABLE user_temp
select
s.user_id ,
s.name ,
s.review_count  ,
s.joined_date,

s.useful_votes_count ,
s.funny_votes_count ,
s.fans_count ,

s.elite ,

s.average_stars_rating ,

s.hot_compliment_count  ,
s.more_compliment_count ,
s.profile_compliment_count ,
s.cute_compliment_count ,
s.list_compliment_count ,
s.note_compliment_count ,
s.plain_compliment_count ,
s.cool_compliment_count ,
s.funny_compliment_count ,
s.writer_compliment_count ,
s.photos_compliment_count ,
s.upd_ts,
current_timestamp() ,
null,
1

from
yelp_dataset_etl.user_dim_final f
JOIN
yelp_dataset_etl.user_dim_stage s
on f.user_id=s.user_id AND
f.scd_active = 1;
"


#Copy all the records from the staging table which don't exist in the production table
query5="
INSERT INTO TABLE user_temp
SELECT
s.*,
current_timestamp() ,
null,
1
from
yelp_dataset_etl.user_dim_stage s
LEFT JOIN
yelp_dataset_etl.user_dim_final f
on f.user_id=s.user_id
WHERE f.user_id is null;
"

# overwrite the final table

query5="
DROP TABLE IF EXISTS yelp_dataset_etl.user_dim_final;" +
create_final + "
INSERT OVERWRITE TABLE yelp_dataset_etl.user_dim_final  select * from user_temp;

"







