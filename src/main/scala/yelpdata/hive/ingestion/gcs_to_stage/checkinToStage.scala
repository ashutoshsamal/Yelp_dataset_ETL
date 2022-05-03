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


  val rawjsonDf = spark.read.option("multiline","true").json("/Users/a0s0iro/Desktop/Yelp_data/test_json/checkin.json")

  val checkinStage=stagedataFrame(rawjsonDf)

  checkinStage.printSchema()
  checkinStage.show(5)

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
