package yelpdata.hive.ingestion.gcs_to_stage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object userToStage extends App {

  val spark=SparkSession.builder()
  .appName("UserStagging")
    .master("local[2]")
    .getOrCreate()


  val rawjsonDf = spark.read.option("multiline","true").json("/Users/a0s0iro/Desktop/Yelp_data/test_json/user.json")


  val userDedup=deduplication(rawjsonDf)

  val userStage=stagedataFrame(userDedup)
  userStage.show(5)



  def stagedataFrame(dedup:DataFrame):DataFrame={
    dedup.select(
      col("user_id"),
      col("name"),
      col("review_count"),
      col("joined_data"),
      col("useful").as("useful_votes_count"),
      col("funny").as("funny_votes_count"),
      col("fans").as("fans_count"),
      col("elite"),
      col("average_stars").as("average_stars_rating"),
      col("compliment_hot").as("hot_compliment_count "),
      col("compliment_more").as("more_compliment_count"),
      col("compliment_profile").as("profile_compliment_count"),
      col("compliment_cute").as("cute_compliment_count"),
      col("compliment_list").as("list_compliment_count"),
      col("compliment_note").as("note_compliment_count"),
      col("compliment_plain").as("plain_compliment_count"),
      col("compliment_cool").as("cool_compliment_count"),
      col("compliment_funny").as("funny_compliment_count"),
      col("compliment_writer").as("writer_compliment_count"),
      col("compliment_photos").as("photos_compliment_count")

    )
  }


  def deduplication(raw_data: DataFrame):DataFrame= {
    raw_data
      .withColumn("joined_data",to_timestamp(col("yelping_since"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("row_num",row_number()over(
        Window.partitionBy(col("user_id")).orderBy(desc("review_count"))))
      .withColumn("elite",split(col("elite"),","))
      .withColumn("friends",split(col("friends"),","))
      .filter("row_num==1")

  }

}
