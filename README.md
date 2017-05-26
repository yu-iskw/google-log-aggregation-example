# Real-Time Log Aggregation on Google Cloud Example

[![Build Status](https://travis-ci.org/yu-iskw/google-log-aggregation-example.svg?branch=master)](https://travis-ci.org/yu-iskw/google-log-aggregation-example)

This is an example of real-time log aggregation with Google Pub/Sub, Google Dataflow/Apache Beam, and Google BigQuery.
When I started learning Google Dataflow/Apache Beam, it is a little difficult to find a way to aggregate logs to date partitioned BigQuery table.
In this repository, I would like to show a solution to realize writing logs to date partitioned BigQuery tables.

This example is implemented in Apache Beam 0.6.0.
As you probably know, [Apache Beam publishes the first stable release](https://beam.apache.org/blog/2017/05/17/beam-first-stable-release.html)
However, it seems that the stable version has not been available by maven yet.
I will try to upgrade this example when the stable version is released.

## How to Run a Job on Google Dataflow

You have to create a Google Cloud project before getting started.
If you don't know how to create one, please following [the instruction](https://cloud.google.com/resource-manager/docs/creating-managing-projects).

### Preparation
1. Make a Google Cloud Storage bucket
2. Make a Google Pub/Sub topic and subscription
3. Make a Google BigQuery dataset ID with cron

Before running a Dataflow job, you must create a Google Cloud Storage bucket for temporary data of Dataflow.
If you haven't installed Google Cloud SDK yet, please install it.
Of course, you can do same operation on Google Cloud web UI.

If you would like to know a little bit more about `gsutil mb`.
[The official documentation](https://cloud.google.com/storage/docs/gsutil/commands/mb)
would be helpful.
```
GCS_BUCKET="your-gcs-bucket"
gsutil mb gs://${GCS_BUCKET}/
```

Then, you must create a Google Pub/Sub topic and subscription.

```
GCP_PROJECT_ID=...
PUBSUB_TOPIC_NAME=...
PUBSUB_SUBSCRIPTION_NAME=...
gcloud beta pubsub topics create --project=$GCP_PROJECT_ID $PUBSUB_TOPIC_NAME
gcloud beta pubsub subscriptions create --project=$GCP_PROJECT_ID $PUBSUB_SUBSCRIPTION_NAME
```

And then, you should make a BigQuery table manually and set a scheduled job to make talbes daily.
As I mentioned above, our goal is to write logs to date-partitioned BigQuery tables whose name following like `event_log_20170101`.
As you can imagine, it is almost impossible and inefficient to make those tables before hand.

```
BQ_DATASET_ID=...
BQ_TABLE_ID=...
SCHEMA="pubsub_timestamp:string,message:string"
PARTITION_DATE=$(date +\%Y\%m\%d)
bq mk --project_id=$GSP_PROJECT_ID \
  --time_partitioning_type="DAY" \
  --schema $SCHEMA \
  "${BQ_DATASET_ID}.${BQ_TABLE_ID}_${PARTITION_DATE}"
```

[`etc/crontab`](./etc/crontab) would be helpful to create a schedule job with cron for creating tables daily.

### Launch Dataflow job

The following command enables us to run a log aggregating job on Google Dataflow.

- `--project`: Google Cloud Project ID
- `--inputPubsubSubscription`: Input Google Pub/Sub subscription name
- `--outputBigQueryDataset`: Output Google BigQuery dataset ID
- `--outputBigQueryTable`: Output Google BigQuery table ID

```
mvn compile exec:java -Dexec.mainClass=com.github.yuiskw.google.LogAggregator \
    -Dexec.args="--runner=DataflowRunner \
    --project=<gcp-project-id> \
    --gcpTempLocation=gs://<your-gcs-bucket>/tmp \
    --inputPubsubSubscription=<subscription> \
    --outputBigQueryDataset=<dataset-id>
    --outputBigQueryTable=<table-id>" \
  -Pdataflow-runner
```

## Java Classes

- `LogAggregator`
    - This class is used for wring log from Google Pub/Sub to BigQuery with Apache Beam/Google Dataflow.
- `TableNameByWindowFn`
    - This class is used for identifying a BigQuery date-partition table prefix which means date.
- `PubsubMessage2TableRowFn`
    - This class is used for converting a Pub/Sub message to a BigQuery table row.

## Links

- [Google Cloud Platform Documentation](https://cloud.google.com/docs/)
- [Learn about Beam](https://beam.apache.org/documentation/)
