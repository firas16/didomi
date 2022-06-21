from collections import Counter
from pyspark.sql.functions import udf
from pyspark.sql.types import MapType, StringType, IntegerType, ArrayType
from pyspark.sql import DataFrame
from pyspark.sql.functions import count, size, when, collect_list, from_json, lit, map_from_arrays
from src.literals import *

def format_input(df: DataFrame)-> DataFrame:
    """
    extracts columns country, user_id from user column
    extracts and infers schema to token column
    :param df:
    :return: dataframe with formatted columns (user_id, country and token)
    """
    # infer schema to token column
    schema = MapType(StringType(), MapType(StringType(), ArrayType(StringType())))

    df_formatted = df \
        .withColumn(country, df.user.country) \
        .withColumn(user_id, df.user.id) \
        .withColumn(token, from_json(df.user.token, schema=schema))
    df_formatted = df_formatted.withColumn(purposes, df_formatted.token.purposes.enabled)
    return df_formatted

def compute_kpis(df: DataFrame, dimensions: list)-> DataFrame:
    """
        compute kpis:
        - pageviews	Metric	Number of events of type pageview
        - pageviews_with_consent	Metric	Number of events of type pageview with consent (ie user.consent = true)
        - consents_asked	Metric	Number of events of type consent.asked
        - consents_asked_with_consent	Metric	Number of events of type consent.asked with consent (ie user.consent = true)
        - consents_given	Metric	Number of events of type consent.given
        - consents_given_with_consent	Metric	Number of events of type consent.given with consent (ie user.consent = true)
        - avg_pageviews_per_user	Metric	Average number of events of type pageview per user

    :param df: dataframe of events
    :param dimensions: dimensions used to group
    :return: dataframe of kpis
    """
    kpis = df \
        .groupby(dimensions) \
        .agg(
        count(when(df.type == "pageview", 1)).alias(pageviews),
        count(when((df.type == "pageview") & (size(df.token.purposes) > 0), 1)).alias(pageviews_with_consent),
        count(when(df.type == "consent.asked", 1)).alias(consents_asked),
        count(when((df.type == "consent.asked") & (size(df.purposes) > 0), 1)).alias(consents_asked_with_consent),
        count(when(df.type == "consent.given", 1)).alias(consents_given),
        count(when((df.type == "consent.given") & (size(df.purposes) > 0), 1)).alias(consents_given_with_consent)
        # collect_list(when((df.type == "pageview"), df.user_id)).alias("avg_page_view_user")
    )
    return kpis

def compute_avg_pageview_per_user(df: DataFrame) -> DataFrame:
    """
    computes average pagesviews per user
    counts pages views per user then constructs a map of {user_id -> avgpageviews}
    :param df:
    :return:
    """
    nb_pages_views = df.count()
    dimensions = [datehour, domain, country, user_id]
    result = df.groupby(dimensions).agg(count(user_id).alias("count_user_id"))
    result = result.withColumn("avg_pageviews_per_user", result.count_user_id / lit(nb_pages_views))
    result = result.groupby([datehour, domain, country])\
        .agg(collect_list(result.user_id).alias("users"), collect_list(result.avg_pageviews_per_user).alias("avg"))
    result = result.withColumn(avg_pageviews_per_user, map_from_arrays("users", "avg"))
    return result.select([datehour, domain, country, avg_pageviews_per_user])

def count_values(ls: list) -> dict:
    return Counter(ls)

#not used (to discuss)
count_values_udf = udf(count_values, MapType(StringType(), IntegerType()))

def test_data(df1: DataFrame, df2: DataFrame):
    """
    compares two dataframes
    :param df1:
    :param df2:
    :return:
    """
    data1 = df1.collect()
    data2 = df2.collect()
    return set(data1) == set(data2)