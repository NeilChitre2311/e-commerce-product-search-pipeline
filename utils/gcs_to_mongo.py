from datetime import date
import pymongo
from pyspark.sql import SparkSession
from user_definition import *


def clean_data(spark, bucket_name, blob_name):
    """
    This functions pulls the data from GCS bucket and maps the data into rdd.
    It takes the spark context, bucket name and file path for the GCS as inputs and returns an rdd.
    """

    df = spark.read.option("multiline", "true").json(f"gs://{bucket_name}/{blob_name}")
    try:
        clean_rdd = df.rdd.map(
            lambda x: {
                "_id": x["id"],
                "name": x["name"],
                "price": float(x["price"]["current"]["value"]),
                "colour": x["colour"],
                "brandName": x["brandName"],
                "categoryId": x["categoryId"],
                "isSellingFast": x["isSellingFast"],
                "facetGroups": x["facetGroupings"],
            }
        )
        print("RDD created successfully!")

    except Exception as e:
        print(e)

    return clean_rdd


def push_to_mongo(mongo_collection, input_data):
    """
    This function pushes rdd data into mongoDB
    """
    try:
        mongo_collection.insert_many(input_data.collect(), ordered = False)
        print("Documents inserted successfully!")
    except Exception as e:
        print(e)


def gcs_to_mongo():

    spark_session = SparkSession.builder.getOrCreate()
    conf = spark_session.sparkContext._jsc.hadoopConfiguration()
    conf.set(
        "google.cloud.auth.service.account.json.keyfile",
        service_account_key_file,
    )
    conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set(
        "fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )
    blob_name = str(date.today()) + "/*.json"
    connection_url = f"mongodb+srv://{mongo_username}:{mongo_password}@{mongo_ip_address}"

    # client = pymongo.MongoClient("localhost", port)
    client = pymongo.MongoClient(connection_url)
    db = client[database_name]
    collection = db[products_collection]

    input_rdd = clean_data(spark_session, bucket_name, blob_name)

    push_to_mongo(collection, input_rdd)
    
    print("Data successfully moved from GCS to MongoDB.")


if __name__ == "__main__":
    gcs_to_mongo()