package yelpdata.hive.ingestion.gcs_to_stage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql._


object userToStage extends App {

  val spark=SparkSession.builder()
  .appName("UserStagging")
    .config("spark.sql.warehouse.dir", "gs://yelp_etl_bucket/stage_tables/")
    .enableHiveSupport()
    .master("yarn")
    .enableHiveSupport()
    .getOrCreate()


  val rawjsonDf = spark.read.option("multiline","true").json("gs://yelp_etl_bucket/test/user_scd test/user_scd.json")


  val userDedup=deduplication(rawjsonDf)

  val user_stagetable=stagedataFrame(userDedup)
  user_stagetable.show(5)
  user_stagetable.printSchema()

  spark.sql("CREATE DATABASE IF NOT EXISTS yelp_dataset_etl;")

  // delete existing table
  spark.sql("DROP TABLE IF EXISTS yelp_dataset_etl.user_dim_stage;")


  // create hive table
  spark.sql("""CREATE TABLE IF NOT EXISTS yelp_dataset_etl.user_dim_stage
              |(
              | user_id string,
              | name string,
              | review_count int ,
              | joined_date timestamp ,
              |
              | useful_votes_count int,
              | funny_votes_count int,
              | fans_count int,
              |
              | elite array<string>,
              |
              | average_stars_rating double ,
              |
              | hot_compliment_count  int,
              | more_compliment_count int,
              | profile_compliment_count int,
              | cute_compliment_count int,
              | list_compliment_count int,
              | note_compliment_count int,
              | plain_compliment_count int,
              | cool_compliment_count int,
              | funny_compliment_count int,
              | writer_compliment_count int,
              | photos_compliment_count int,
              |
              | upd_ts timestamp
              |
              |)
              |stored as parquet
              |LOCATION
              |'gs://yelp_etl_bucket/stage_tables/user';""".stripMargin)

  
  
  //loading to stage hive table
  user_stagetable.write.format("hive")
    .mode(SaveMode.Overwrite)
    .insertInto("yelp_dataset_etl.user_dim_stage")

  def stagedataFrame(dedup:DataFrame):DataFrame={
    dedup.select(
      col("user_id"),
      col("name"),
      col("review_count"),
      col("joined_date"),
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
      .withColumn("upd_ts",current_timestamp())
  }


  def deduplication(raw_data: DataFrame):DataFrame= {
    raw_data
      .withColumn("joined_date",to_timestamp(col("yelping_since"),"yyyy-MM-dd HH:mm:ss"))
      .withColumn("row_num",row_number()over(
        Window.partitionBy(col("user_id")).orderBy(desc("review_count"))))
      .withColumn("elite",split(col("elite"),","))
      .withColumn("friends",split(col("friends"),","))
      .filter("row_num==1")

  }

}
