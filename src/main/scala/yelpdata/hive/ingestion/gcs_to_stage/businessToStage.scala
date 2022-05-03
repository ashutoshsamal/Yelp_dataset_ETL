package yelpdata.hive.ingestion.gcs_to_stage

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import org.apache.spark.sql._

object businessToStage extends App{

  val spark=SparkSession.builder()
    .appName("buisnessStagging")
    .master("yarn")
    .getOrCreate()

  // Reading raw json to the DataFrame
  val rawjsonDf = spark.read.option("multiline","true").json("/Users/a0s0iro/Desktop/Yelp_data/test_json/buisness_test.json")

  //Removing duplicate records if any
  val business_dedup=deduplication(rawjsonDf)

  //creating the DataFrame for stage table
  val business_stagetable=stageDataframe(business_dedup)


  business_stagetable.printSchema()
  business_stagetable.show(50)

  //loading to stage hive table
  business_stagetable.write.format("hive")
    .mode(SaveMode.Overwrite)
    .insertInto("yelp_dataset_etl.business_dim_stage")



  def stageDataframe(dedup: DataFrame):DataFrame= {
       dedup
         .withColumn("monday_open_time",date_format(element_at(col("monday"),1),"HH:mm:ss"))
         .withColumn("monday_close_time",date_format(element_at(col("monday"),2),"HH:mm:ss"))
         .withColumn("tuesday_open_time",date_format(element_at(col("tuesday"),1),"HH:mm:ss"))
         .withColumn("tuesday_close_time",date_format(element_at(col("tuesday"),2),"HH:mm:ss"))
         .withColumn("wednesday_open_time",date_format(element_at(col("Wednesday"),1),"HH:mm:ss"))
         .withColumn("wednesday_close_time",date_format(element_at(col("Wednesday"),2),"HH:mm:ss"))
         .withColumn("thursday_open_time",date_format(element_at(col("thursday"),1),"HH:mm:ss"))
         .withColumn("thursday_close_time",date_format(element_at(col("thursday"),2),"HH:mm:ss"))
         .withColumn("friday_open_time",date_format(element_at(col("friday"),1),"HH:mm:ss"))
         .withColumn("friday_close_time",date_format(element_at(col("friday"),2),"HH:mm:ss"))
         .withColumn("saturday_open_time",date_format(element_at(col("saturday"),1),"HH:mm:ss"))
         .withColumn("saturday_close_time",date_format(element_at(col("saturday"),2),"HH:mm:ss"))
         .withColumn("sunday_open_time",date_format(element_at(col("sunday"),1),"HH:mm:ss"))
         .withColumn("sunday_close_time",date_format(element_at(col("sunday"),2),"HH:mm:ss"))
         .withColumn("upd_ts",current_timestamp())
         .drop("monday","tuesday","Wednesday","thursday","friday","saturday","sunday","row_num")



  }


  def deduplication(raw_data: DataFrame):DataFrame={
  raw_data.withColumn("categories",split(col("categories"),",")).withColumn("category",explode(col("categories"))).
    select(
      col("business_id"),
      col("name").as("business_name"),
      col("category"),
      col("address"),
      col("city"),
      col("state"),
      col("postal_code").as("zip_code"),
      col("latitude"),
      col("longitude"),
      col("stars").as("star_rating"),
      col("review_count"),
      col("is_open").as("open_ind"),
      split(col("hours.Monday"),"-").as("monday"),
      split(col("hours.Tuesday"),"-").as("tuesday"),
      split(col("hours.Wednesday"),"-").as("Wednesday"),
      split(col("hours.Thursday"),"-").as("thursday"),
    split(col("hours.Friday"),"-").as("friday"),
    split(col("hours.Saturday"),"-").as("saturday"),
    split(col("hours.Sunday"),"-").as("sunday"),
    row_number().over(
      Window.partitionBy(col("business_id"),col("categories")).orderBy("review_count")).as("row_num")
    ).filter("row_num==1").orderBy("business_name")
  }
}
