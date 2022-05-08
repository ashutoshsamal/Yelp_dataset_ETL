package yelpdata.hive.ingestion.gcs_to_stage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object tipToStage extends App {

  val spark=SparkSession.builder()
    .appName("TipStagging")
    .master("yarn")
    .getOrCreate()

  val rawjsonDf = spark.read.option("multiline","true").json("gs://yelp_etl_bucket/test_json/tip.json")

  val tipStagetable=stagedataFrame(rawjsonDf)
  tipStagetable.printSchema()
  tipStagetable.show(5)


  // delete existing table
  spark.sql("DROP TABLE IF EXISTS yelp_dataset_etl.tip_fact_stage;")

  // create hive table
  spark.sql("""CREATE  TABLE IF NOT EXISTS yelp_dataset_etl.tip_fact_stage
              |(
              |business_id string,
              |user_id string,
              |compliment_count integer,
              |tip_timestamp timestamp,
              |text string,
              |upd_ts timestamp
              |)
              |stored as parquet
              |LOCATION
              |'gs://yelp_etl_bucket/stage_tables/tip';""".stripMargin)


  //loading to stage hive table
  tipStagetable.write.format("hive")
    .mode(SaveMode.Overwrite)
    .insertInto("yelp_dataset_etl.tip_fact_stage")


  def stagedataFrame(raw_data: DataFrame):DataFrame= {
    raw_data
      .withColumn("tip_timestamp",to_timestamp(col("date"),"yyyy-MM-dd HH:mm:ss"))
      .select(
        col("business_id"),
        col("user_id"),
        col("compliment_count"),
        col("tip_timestamp"),
        col("text")
      )
      .withColumn("upd_ts",current_timestamp())
  }



}
