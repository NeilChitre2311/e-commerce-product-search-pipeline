from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.feature import Normalizer
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
import pyspark
from pyspark import SparkContext
from pyspark.ml.feature import (
    IDF,
    HashingTF,
    Normalizer,
    RegexTokenizer,
    StopWordsRemover,
)
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import FloatType

# TODO: Read from os.environment
mongo_uri = "mongodb://localhost:27017"
mongo_db = "msds697"
mongo_collection = "products"

def get_spark_objects():
    conf = (
        pyspark.SparkConf()
        .set(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        )
        .setMaster("local")
        .setAppName("Pre-processing")
    )
    sc = SparkContext(conf=conf)
    sql_c = SQLContext(sc)
    ss = SparkSession.builder.appName("SearchEngine").getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    return sc, sql_c, ss

def compute_vectors(search_query, data, ss):
    search_df = ss.createDataFrame([(search_query,)], ["name"])
    
    tokenizer = RegexTokenizer(pattern="\\W+", inputCol="name", outputCol="words")
    stop_words = StopWordsRemover.loadDefaultStopWords("english")
    remover = StopWordsRemover(
        inputCol="words", outputCol="filtered_words", stopWords=stop_words
    )
    
    processed_query = tokenizer.transform(search_df)
    processed_query = remover.transform(processed_query)
    
    processed_data = tokenizer.transform(data)
    processed_data = remover.transform(processed_data)
    processed_data = processed_data.select('_id', 'name', 'filtered_words')
    
    hashingTF = HashingTF(inputCol="filtered_words", outputCol="rawFeatures", numFeatures=100)
    featurizedQuery = hashingTF.transform(processed_query)
    featurizedData = hashingTF.transform(processed_data)
    
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    
    rescaledData = idfModel.transform(featurizedData)
    rescaledQuery = idfModel.transform(featurizedQuery)
    
    normalizer = Normalizer(inputCol="features", outputCol="norm")
    normalizedData = normalizer.transform(rescaledData)
    normalizedQuery = normalizer.transform(rescaledQuery)
    
    return normalizedQuery, normalizedData

def fetch_data_from_mongodb(sql_c):
    data = (
        sql_c.read.format("com.mongodb.spark.sql.DefaultSource")
        .option("spark.mongodb.input.uri", f"{mongo_uri}/{mongo_db}.{mongo_collection}")
        .load()
    )
    return data

def cosine_similarity(vec1, vec2):
    dot_product = float(vec1.dot(vec2))
    norm_product = float(vec1.norm(2) * vec2.norm(2))
    similarity = dot_product / norm_product if norm_product != 0 else 0.0
    return similarity

if __name__ == "__main__":
    #TODO: Create a prompt to read input
    search_query = "sneakers"
    sc, sql_c, ss = get_spark_objects()
    data = fetch_data_from_mongodb(sql_c)
    nq, nd = compute_vectors(search_query, data, ss)
    search_v = nq.select('norm').collect()[0][0]
    cosine_similarity_udf2 = udf(lambda x: cosine_similarity(search_v,x), FloatType())
    similarity_df = nd.withColumn("similarity", cosine_similarity_udf2(nd["norm"]))
    similar_products = similarity_df.orderBy(desc("similarity")).select('name', 'similarity').limit(10).collect()
    print('**********************************')
    print(f'You might like these products: {similar_products}')
