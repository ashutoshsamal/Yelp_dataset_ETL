ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.10"

lazy val root = (project in file("."))
  .settings(
    name := "Yelp_dataset_ETL"
  )
val sparkVersion="2.4.8"

libraryDependencies ++=Seq(
  "org.apache.spark" % "spark-core_2.12" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.12" % sparkVersion,
  "org.apache.spark" % "spark-hive_2.12" % sparkVersion
)