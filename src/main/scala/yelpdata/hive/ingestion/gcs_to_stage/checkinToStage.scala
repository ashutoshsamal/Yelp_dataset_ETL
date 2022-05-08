package yelpdata.hive.ingestion.gcs_to_stage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object checkinToStage extends  App {

  val spark=SparkSession.builder()
    .appName("CheckinStagging")
    .master("yarn")
    .getOrCreate()

  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val rawjsonDf = spark.read.option("multiline","true").json("gs://yelp_etl_bucket/test_json/checkin.json")

  val checkinStage=stagedataFrame(rawjsonDf)

  checkinStage.printSchema()
  checkinStage.show(5)


  // delete existing table
  spark.sql("DROP TABLE IF EXISTS yelp_dataset_etl.checkin_stage;")

  // create hive table
  spark.sql("""CREATE TABLE IF NOT EXISTS yelp_dataset_etl.checkin_stage
              |(
              |business_id string,
              |checkin_time_stamp timestamp,
              |cchekin_year int,
              |checkin_month int,
              |checkin_day int,
              |upd_ts timestamp
              |)
              |stored as parquet
              |LOCATION
              |'gs://yelp_etl_bucket/stage_tables/checkin';""".stripMargin)



  //loading to stage hive table
  checkinStage.write.format("hive")
    .mode(SaveMode.Overwrite)
    .insertInto("yelp_dataset_etl.checkin_stage")

  def  stagedataFrame(raw_data:DataFrame):DataFrame={
    raw_data.withColumn("date",split(col("date"),","))
      .withColumn("checkin_time_stamp",explode(col("date")))
      .withColumn("checkin_time_stamp",to_timestamp(col("checkin_time_stamp"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("chekin_year",year(col("checkin_time_stamp")))
      .withColumn("checkin_month",month(col("checkin_time_stamp")))
      .withColumn("checkin_day",dayofmonth(col("checkin_time_stamp")))
      .withColumn("upd_ts",current_timestamp())
      .drop("date")

  }

}
