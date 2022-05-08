
# run the shell script stored is gs bucket by - gsutil cat gs://yelp_etl_bucket/codes/spark_submit.sh | sh -s yelpdata.hive.ingestion.gcs_to_stage.businessToStage

class=$1
gcloud dataproc jobs submit spark --cluster=yelp-etl \
--region=us-central1 \
--jars=gs://yelp_etl_bucket/codes/yelp_dataset_etl_2.12-0.1.0-SNAPSHOT.jar \
--class=$class