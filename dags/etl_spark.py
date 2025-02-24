import os
import boto3

from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.models import Variable

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

os.environ["AWS_ACCESS_KEY_ID"] = Variable.get("AWS_ACCESS_KEY_ID_SECRET")
os.environ["AWS_SECRET_ACCESS_KEY"] = Variable.get("AWS_SECRET_ACCESS_KEY_SECRET")
os.environ["AWS_DEFAULT_REGION"] = Variable.get("AWS_DEFAULT_REGION")
os.environ["AWS_ENDPOINT_URL"] = Variable.get("AWS_ENDPOINT_URL")
os.environ["NB_SPARK_SESSION_ENDPOINT"] = Variable.get("NB_SPARK_SESSION_ENDPOINT")
os.environ["NB_SPARK_CLUSTER_PASSWORD"] = Variable.get("NB_SPARK_SESSION_PASSWORD_SECRET")
os.environ["NB_SPARK_SESSION_ROOT_CERTIFICATES_FILE"] = Variable.get("NB_SPARK_SESSION_ROOT_CERTIFICATES_FILE")

RAW_LANDING_BUCKET = 'sai-raw'
RAW_PROCESSING_BUCKET = 'sai-processing'
PROCESSED_BUCKET = 'sai-processed'
ARCHIVE_BUCKET = 'sai-archive'

schema = StructType([
    StructField("collector_id", StringType(), True),
    StructField("collector_tstamp", TimestampType(), True),
    StructField("event_type", StringType(), True),
    StructField("payload", StringType(), True),
    StructField("root_id", StringType(), True),
    StructField("schema_version", StringType(), True)
])

schema_yellow_taxi_0_1_0 = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", FloatType(), True),
    StructField("trip_distance", FloatType(), True),
    StructField("RatecodeID", FloatType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", FloatType(), True),
    StructField("fare_amount", FloatType(), True),
    StructField("extra", FloatType(), True),
    StructField("mta_tax", FloatType(), True),
    StructField("tip_amount", FloatType(), True),
    StructField("tolls_amount", FloatType(), True),
    StructField("improvement_surcharge", FloatType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("congestion_surcharge", FloatType(), True),
    StructField("Airport_fee", FloatType(), True)
])

schema_green_taxi_0_1_0 = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("lpep_pickup_datetime", TimestampType(), True),
    StructField("lpep_dropoff_datetime", TimestampType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("RatecodeID", FloatType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("passenger_count", FloatType(), True),
    StructField("trip_distance", FloatType(), True),
    StructField("fare_amount", FloatType(), True),
    StructField("extra", FloatType(), True),
    StructField("mta_tax", FloatType(), True),
    StructField("tip_amount", FloatType(), True),
    StructField("tolls_amount", FloatType(), True),
    StructField("ehail_fee", FloatType(), True),
    StructField("improvement_surcharge", FloatType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("payment_type", FloatType(), True),
    StructField("trip_type", FloatType(), True),
    StructField("congestion_surcharge", FloatType(), True)
])

schema_map = {"YellowTaxiTripRecords_0-1-0": schema_yellow_taxi_0_1_0,
             "GreenTaxiTripRecords_0-1-0": schema_green_taxi_0_1_0}


def move_objects(source_bucket, destination_bucket):
    # List all objects in the source bucket
    response = s3.list_objects_v2(Bucket=source_bucket)

    if 'Contents' not in response:
        print("No objects found in the source bucket.")
        return

    for obj in response['Contents']:
        key = obj['Key']
        print(f"Moving: {key}")

        # Copy the object to the destination bucket
        copy_source = {'Bucket': source_bucket, 'Key': key}
        s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=key)

        # Delete the object from the source bucket after copying
        s3.delete_object(Bucket=source_bucket, Key=key)

        print(f"Moved {key} to {destination_bucket}")

    print("All objects have been moved.")

    return "Success"


def archive_objects(source_bucket, destination_bucket, etl_timestamp):
    # List all objects in the source bucket
    response = s3.list_objects_v2(Bucket=source_bucket)

    if 'Contents' not in response:
        print("No objects found in the source bucket.")
        return

    for obj in response['Contents']:
        key = obj['Key']
        print(f"Moving: {key}")

        # Copy the object to the destination bucket
        copy_source = {'Bucket': source_bucket, 'Key': f"{etl_timestamp}/{key}"}
        s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=f"{etl_timestamp}/{key}")

        # Delete the object from the source bucket after copying
        s3.delete_object(Bucket=source_bucket, Key=key)

        print(f"Moved {key} to {destination_bucket}")

    print("All objects have been moved.")

    return "Success"


def process_data(etl_timestamp):

    from pyspark.sql.connect.session import SparkSession
    from nebius.spark.connect import create_channel_builder
    from os.path import expanduser
    from pyspark.sql.functions import from_json, col, lit

    import urllib.request

    url = "https://storage.eu-north1.nebius.cloud/msp-certs/ca.pem"

    urllib.request.urlretrieve(url, "ca.pem")

    nebius_spark_endpoint = os.environ["NB_SPARK_SESSION_ENDPOINT"] + ':443'
    nebius_spark_cb = create_channel_builder(
        nebius_spark_endpoint,
        password=os.environ["NB_SPARK_CLUSTER_PASSWORD"],
        root_certificates_file=expanduser('ca.pem') 
    )

    spark = (SparkSession
        .builder
        .channelBuilder(nebius_spark_cb)
        .getOrCreate())

    df_raw = spark.read.schema(schema).json(f"s3a://{RAW_PROCESSING_BUCKET}/")
    distinct_groups = df_raw.select("event_type", "schema_version").distinct().collect()

    for (event_type, schema_version) in [(str(row["event_type"]), str(row["schema_version"])) for row in distinct_groups]:
        (df_raw
        .filter(df_raw.event_type == event_type)
        .filter(df_raw.schema_version == schema_version)
        .withColumn('payload', from_json(col('payload'), schema_map.get(f"{event_type}_{schema_version}")))
        .withColumn('etl_timestamp', lit(etl_timestamp))
        .select("etl_timestamp", "collector_id", "collector_tstamp", "event_type", "root_id", "schema_version", "payload.*")
        .write
        .partitionBy("etl_timestamp")
        .parquet(f"s3a://{PROCESSED_BUCKET}/{event_type}/{schema_version.split('-')[0]}"))

    return "Success"


time_now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')

s3 = boto3.client('s3')

with DAG(dag_id="spark_etl", start_date=datetime(2025, 1, 24), schedule="*/5 * * * *") as dag:

    @task()
    def move_data_to_processing():
        move_objects(RAW_LANDING_BUCKET, RAW_PROCESSING_BUCKET)

    @task()
    def run_processing():
        process_data(time_now)

    @task()
    def clean_procesing():
        archive_objects(RAW_PROCESSING_BUCKET, ARCHIVE_BUCKET, time_now)

    move_data_to_processing() >> run_processing() >> clean_procesing()