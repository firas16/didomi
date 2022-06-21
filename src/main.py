from pyspark.sql import SparkSession
from src.utils import compute_avg_pageview_per_user, compute_kpis, format_input
from src.literals import *

#instantiate spark session
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


# Read JSON file into dataframe
path = "test/resources/input"
df = spark.read.json(path)
df.printSchema()

#drop duplicates with the same event id
df = df.drop_duplicates(subset=[id])

#infer schema to token column and format dataset
df = format_input(df)

#compute KPIs
dimensions = [domain, datehour, country]
kpis = compute_kpis(df, dimensions)

# compute avg pageviews per user
avg_page_views = compute_avg_pageview_per_user(df)

# join two KPI dataframes
result = kpis.join(avg_page_views, on=dimensions, how="inner")

result.write.parquet("test/resources/output/kpis")



