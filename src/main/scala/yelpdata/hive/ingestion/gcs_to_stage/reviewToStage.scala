package yelpdata.hive.ingestion.gcs_to_stage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object reviewToStage extends App {

  val spark=SparkSession.builder()
    .appName("ReviewStagging")
    .config("spark.sql.warehouse.dir", "gs://yelp_etl_bucket/stage_tables/")
    .enableHiveSupport()
    .master("yarn")
    .getOrCreate()


  // Reading raw json to the DataFrame
  val rawjsonDf = spark.read.option("multiline","true").json("gs://yelp_etl_bucket/test_json/review.json")

  //Removing duplicate records if any
  val review_dedup=deduplication(rawjsonDf)


  //creating the DataFrame for stage table
   val review_stage=stagedataFrame(review_dedup)

    review_stage.show(5)
  review_stage.printSchema()


  // delete existing table
  spark.sql("DROP TABLE IF EXISTS yelp_dataset_etl.reviews_fact_stage;")

  // create hive table
  spark.sql("""CREATE TABLE IF NOT EXISTS yelp_dataset_etl.reviews_fact_stage
              |(
              |
              |review_id string,
              |user_id string,
              |business_id string,
              |stars float,
              |review_text string,
              |useful_votes_count int,
              |funny_votes_count int,
              |cool_votes_count int,
              |review_timestamp timestamp,
              |review_month integer,
              |review_year integer,
              |review_day integer,
              |upd_ts timestamp
              |)
              |stored as parquet
              |LOCATION
              |'gs://yelp_etl_bucket/stage_tables/review';""".stripMargin)


  //loading to stage hive table
  review_stage.write.format("hive")
    .mode(SaveMode.Overwrite)
    .insertInto("yelp_dataset_etl.reviews_fact_stage")




  def stagedataFrame(dedub:DataFrame):DataFrame={
    dedub
      .withColumn("review_month",month(col("review_timestamp")))
      .withColumn("review_year",year(col("review_timestamp")))
      .withColumn("review_day",dayofmonth(col("review_timestamp"))) // 1 being sunday and 7 saturday
      .withColumn("upd_ts",current_timestamp())
      .drop("row_num")


  }

  def deduplication(raw_data: DataFrame):DataFrame= {
    raw_data.withColumn("review_timestamp",to_timestamp(col("date"),"yyyy-MM-dd HH:mm:ss")).select(
      col("review_id"),
      col("user_id"),
      col("business_id"),
      col("stars"),
      col("text").as("review_text"),
      col("useful").as("useful_votes_count"),
      col("funny").as("funny_votes_count"),
      col("cool").as("cool_votes_count"),
      col("review_timestamp"),
      row_number().over(
        Window.partitionBy(col("review_id"),to_date(col("review_timestamp"),"yyyy-MM-dd")).orderBy(desc("review_timestamp"))).as("row_num")
    ).select("*").filter("row_num==1")
  }


}
