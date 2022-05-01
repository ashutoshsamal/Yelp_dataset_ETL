package yelpdata.hive.ingestion.gcs_to_stage

import org.apache.spark.sql.SparkSession

object buisnessToStage extends App{

  val spark=SparkSession.builder()
    .appName("buisnessStagging")
    .master("local[2]")
    .getOrCreate()


  val rawjsonDf = spark.read.option("multiline","true").json("gs://yelp_etl_bucket/test/academic_dataset_business_part1.json")

}
