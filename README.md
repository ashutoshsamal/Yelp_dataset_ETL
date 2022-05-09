# DATA ENGINEERING ON YELP DATASET






# SUMMERY

This is an end to end data engineering project for YELP data set (https://www.yelp.com/dataset/documentation/main) that I 
build to get a hands-on experience on spark and hadoop ecosystem.
I have used scala as a programing language , spark as compute engine and hive as warehouse and
used data proc cluster in GCP to deploy my code. 

# DATASET

Yelp data set contains five types of data Business,review,user,checkin,tip. I tried to cover different ways to process
data so followed different ways of ETL for all this tables as follows:-

## Business:-
business.json file have the following structure
   
{

    // string, 22 character unique string business id
    " business_id": "tnhfDv5Il8EaGSXZGiuQGg",

    // string, the business's name
    "name": "Garaje",

    // string, the full address of the business
    "address": "475 3rd St",

    // string, the city
    "city": "San Francisco",

    // string, 2 character state code, if applicable
    "state": "CA",

    // string, the postal code
    "postal code": "94107",

    // float, latitude
    "latitude": 37.7817529521,

    // float, longitude
    "longitude": -122.39612197,

    // float, star rating, rounded to half-stars
    "stars": 4.5,

    // integer, number of reviews
    "review_count": 1198,

    // integer, 0 or 1 for closed or open, respectively
    "is_open": 1,

    // object, business attributes to values. note: some attribute values might be objects
    "attributes": {
        "RestaurantsTakeOut": true,
        "BusinessParking": {
            "garage": false,
            "street": true,
            "validated": false,
            "lot": false,
            "valet": false
        },
    },

    // an array of strings of business categories
    "categories": [
        "Mexican",
        "Burgers",
        "Gastropubs"
    ],

    // an object of key day to value hours, hours are using a 24hr clock
    "hours": {
        "Monday": "10:00-21:00",
        "Tuesday": "10:00-21:00",
        "Friday": "10:00-21:00",
        "Wednesday": "10:00-21:00",
        "Thursday": "10:00-21:00",
        "Sunday": "11:00-18:00",
        "Saturday": "10:00-21:00"
    }
}



Let's make some assumption for our project

### How we receive the data ?

Let's assume a json file is created as soon as there is any changes in the existing business data 
(i.e any changes in star rating,category etc) or if any new business is added and this json files are pushed to any 
kafka topic and we dumped the data from kafka topic to the gs bucket for futher process.

### Moving data from gcs bucket to hive stage table
-> I exploded the category column and chose business_id and category as my grain so we might have repeated  business_ids 
but combination of business id and category will be unique

-> As json files are getting created as soon as there is some changes in the existing data there is chance of duplicate 
records in the kafka topic if any business have multiple changes between two runs of our ETL.

-> So I did deduplication of data after reading it in spark 
(/src/main/scala/yelpdata/hive/ingestion/gcs_to_stage/businessToStage.scala)

-> After the ETL, loaded the data into the stage hive table for incremental load of final  table

### Inremental load of final table

-> As we have delta data(i.e All changes from the last run) in the stage table we can perform incremental load and 
update the final tabel

-> I have maintained a final snapshot table to store the current state of all the records we update the records in this
  table incrementally.

--> hive command for this steps : /src/main/scala/yelpdata/hive/ingestion/stage_to_final/business_stg_to_final_hive.sh

### Preserving historical changes of the data

-> As we update the table in every run ,we are losing the historical data .So I have created a historical table to 
with same schema as the final table but with 3 nested partition(year,month,day)

-> We load this table every day/week from the snapshot table(i.e current state) to the appropriate partition.

-> Hive command for this : /src/main/scala/yelpdata/hive/ingestion/stage_to_final/business_stg_to_final_hive.sh












