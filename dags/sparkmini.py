import csv
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import day_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from minio import Minio
import pandas as pd
import io
import logging

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# MinIO client configuration
minio_client = Minio(
    "minio:9000",  # Chú ý: Port của MinIO thường là 9000
    access_key="minio_access_key",
    secret_key="minio_secret_key",
    secure=False
)

# DAG definition
with DAG(
    dag_id="expanded_spark_etl_minio_dag",
    default_args=default_args,
    description="An expanded ETL workflow with Spark and MinIO using TaskFlow API",
    schedule_interval=None,
    start_date=day_ago(1),
    catchup=False
) as dag:

    @task
    def extract():
        # Extract data from a PostgreSQL database
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        sql = "SELECT * FROM source_table"
        df = pg_hook.get_pandas_df(sql)
        logging.info(f"Extracted {len(df)} rows from source database")
        return df

    @task
    def transform(df: pd.DataFrame):
        # Save data frame to CSV for Spark processing
        csv_path = '/tmp/input_data.csv'
        df.to_csv(csv_path, index=False)
        logging.info(f"Data saved to {csv_path} for Spark processing")
        return csv_path

    spark_job = SparkSubmitOperator(
        task_id="spark_transform",
        application="./path/include/spark.py",
        conn_id="spark_default", #must connect Spark in Airflow Connection
        application_args=['/tmp/input_data.csv', '/tmp/transformed_data.csv'],
        verbose=True
    )

    @task
    def load():
        # Load data from CSV to MinIO
        transformed_csv_path = '/tmp/transformed_data.csv'
        df_transformed = pd.read_csv(transformed_csv_path)
        csv_data = df_transformed.to_csv(index=False).encode('utf-8')
        minio_client.put_object(
            bucket_name="my-bucket",
            object_name="output/transformed_data.csv",
            data=io.BytesIO(csv_data),
            length=len(csv_data),
            content_type='application/csv'
        )
        logging.info("Transformed data loaded successfully into MinIO")

    # Define DAG execution order
    extracted_data = extract()
    csv_path = transform(extracted_data)
    csv_path >> spark_job >> load()
