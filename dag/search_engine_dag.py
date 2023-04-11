import os
from datetime import datetime
from product_indexing import fetch_products
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="search_engine-airflow",
    start_date=datetime(2023, 2, 22),
    schedule_interval="@daily",
) as dag:

    os.environ["no_proxy"] = "*"

    get_products_op = PythonOperator(
        task_id="get_products_data", python_callable=fetch_products
    )

    gcs_to_mongo_op = SparkSubmitOperator(
        task_id="gcs_to_mongo_data",
        packages="com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        exclude_packages="javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri",
        conf={
            "spark.driver.userClassPathFirst": True,
            "spark.executor.userClassPathFirst": True,
        },
        verbose=True,
        application="airflow/dags/pySparkJob.py",
    )

    get_products_op >> gcs_to_mongo_op