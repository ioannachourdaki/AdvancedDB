from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
import datetime

#Imports for query1
from pyspark.sql.functions import month, year, count, desc, rank, countDistinct, regexp_replace
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Query 1") \
    .getOrCreate()

crime_df = spark.read.parquet("output/crime_df.parquet")

#################### QUERY 1 ###############################

## SQL API ###

start_time_sql = datetime.datetime.now()

#remove the rows with null values in DATE OCC (there are none)
crime_df_na_DateOCC=crime_df.na.drop(subset=["DATE OCC"])
print("Total number of rows df after na:", crime_df_na_DateOCC.count())
crime_df_na_DateOCC.createOrReplaceTempView("crimes_q1")

query1 = """
    SELECT year, month, crime_total, rank_within_year
    FROM (
        SELECT year, month, crime_total,
               RANK() OVER (PARTITION BY year ORDER BY crime_total DESC) AS rank_within_year
        FROM (
            SELECT year(`DATE OCC`) AS year, month(`DATE OCC`) AS month,
                COUNT(*) AS crime_total
            FROM crimes_q1
            GROUP BY year(`DATE OCC`), month(`DATE OCC`)
        ) ranked
    ) ranked_with_ranks
    WHERE rank_within_year <= 3
    ORDER BY year
"""

result1_SQL=spark.sql(query1)
print("For each year the months with the most crimes are:")
print("With SQL API:")

result1_SQL.show(result1_SQL.count(), False)

end_time_sql = datetime.datetime.now()


### DataFrame API ###

start_time_df = datetime.datetime.now()

result1_DF = crime_df.groupBy(year('DATE OCC').alias('year'), month('DATE OCC').alias('month')).agg(count('*').alias('crime_total'))

# Define a Window partitioned by year and ordering by the number of crimes per month in descending order
window_spec = Window.partitionBy('year').orderBy(desc('crime_total'))

result1_DF = result1_DF.withColumn('rank_within_year', rank().over(window_spec))
result1_DF = result1_DF.where(col('rank_within_year') <= 3)
result1_DF = result1_DF.orderBy('year', desc('crime_total'))

print("With DF API:")
result1_DF.show(result1_DF.count(), False)

end_time_df = datetime.datetime.now()

execution_time_sql = end_time_sql - start_time_sql
execution_time_df = end_time_df - start_time_df

# Display execution times
print(f"SQL Execution Time: {execution_time_sql} seconds")
print(f"DataFrame Execution Time: {execution_time_df} seconds")
spark.stop()