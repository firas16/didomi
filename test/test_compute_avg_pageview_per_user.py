from pyspark.sql import SparkSession
from src.utils import compute_avg_pageview_per_user
from src.utils import test_data
from src.literals import *


def test_filter_spark_data_frame_by_value():
    # Spark Context initialisation
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .getOrCreate()

    # Given
    input = spark.read.parquet("resources/sample").drop_duplicates(subset=['id'])

    #When
    result = compute_avg_pageview_per_user(df=input)
    data = [["2021-01-23-10", "www.domain-A.eu", "FR", [{"1705c98b-367c-6d09-a30f-da9e6f4da700" : 1.0}]]]
    cols = [datehour, domain, country, avg_pageviews_per_user]
    expected = spark.createDataFrame(data, cols)

    #Then
    assert test_data(result, expected)

