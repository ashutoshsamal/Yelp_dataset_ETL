package yelpdata.hive.ingestion.gcs_to_stage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
object reviewToStage extends App {

  val spark=SparkSession.builder()
    .appName("ReviewStagging")
    .master("local[2]")
    .getOrCreate()


  // Reading raw json to the DataFrame
  val rawjsonDf = spark.read.option("multiline","true").json("/Users/a0s0iro/Desktop/Yelp_data/data/Split_data/review_data/academic_dataset_review_part2.json")

  //Removing duplicate records if any
  val review_dedup=deduplication(rawjsonDf)


  //creating the DataFrame for stage table
   val review_stage=stagedataFrame(review_dedup)

    review_stage.show(5)
  review_stage.printSchema()




  def stagedataFrame(dedub:DataFrame):DataFrame={
    dedub
      .withColumn("review_month",month(col("review_date")))
      .withColumn("review_year",year(col("review_date")))
      .withColumn("review_day",dayofweek(col("review_date"))) // 1 being sunday and 7 saturday
      .drop("row_num")


  }

  def deduplication(raw_data: DataFrame):DataFrame= {
    raw_data.withColumn("review_date",to_date(col("date"),"yyyy-MM-dd")).select(
      col("review_id"),
      col("user_id"),
      col("business_id"),
      col("stars"),
      col("text").as("review_text"),
      col("useful").as("useful_votes_count"),
      col("funny").as("funny_votes_count"),
      col("cool").as("cool_votes_count"),
      col("review_date"),
      row_number().over(
        Window.partitionBy(col("review_id"),col("review_date")).orderBy("review_date")).as("row_num")
    ).select("*").filter("row_num==1")
  }


}
