# DATA ENGINEERING ON YELP DATASET






# SUMMERY

This is an end to end data engineering project for YELP data set (https://www.yelp.com/dataset/documentation/main) that I 
build to get a hands-on experience on spark and hadoop ecosystem.
I have used scala as a programing language , spark as compute engine and hive as warehouse and
used data proc cluster in GCP to deploy my code. 

# DATASET

Yelp data set contains five types of data Business,review,user,checkin,tip. I tried to cover different ways to process
data so followed different ways of ETL for all this tables as follows:-

### Business:-
business.json file have following structure
    // sas
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







