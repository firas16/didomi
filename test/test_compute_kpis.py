from pyspark.sql import SparkSession
from src.utils import compute_kpis
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
    result = compute_kpis(df=input, dimensions=[datehour, domain, country])
    data = [["2021-01-23-10", "www.domain-A.eu", "FR", 4, 3, 1, 0, 3, 3]]
    cols = [datehour, domain, country, pageviews, pageviews_with_consent, consents_asked,
            consents_asked_with_consent, consents_given, consents_given_with_consent]
    expected = spark.createDataFrame(data, cols)

    #Then
    assert test_data(result, expected)

