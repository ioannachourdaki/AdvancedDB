from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType
from pyspark.sql.functions import col, to_date
import datetime

#Imports for query1,3
from pyspark.sql.functions import year, count, desc, countDistinct, regexp_replace, broadcast

spark = SparkSession \
    .builder \
    .appName("Part 2") \
    .getOrCreate()

spark.conf.set("spark.sql.debug.maxToStringFields", "100")
crime_df = spark.read.parquet("output/crime_df.parquet")

####################QUERY 3###############################

LA_income_2015=StructType([
    StructField("Zip Code", IntegerType()),
    StructField("Community", StringType()),
    StructField("Estimated Median Income", StringType())
])
income_df= spark.read.csv("hdfs:///user/user/LA_income_2015.csv", header=True, schema= LA_income_2015)

#we read 'Estimated Median Income' as string bc there are the '$', ',' characters, we remove them and we convert the type to integer, bc we will use this colummn in orderBy later
income_df = income_df.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"), "[\$,]", ""))
income_df = income_df.withColumn("Estimated Median Income", col("Estimated Median Income").cast("integer"))

Revgecoding=StructType([
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType()),
    StructField("ZIPcode", IntegerType())
])
revgecoding_df= spark.read.csv("hdfs:///user/user/revgecoding.csv", header=True, schema= Revgecoding)

start_time = datetime.datetime.now()

revgecoding_df=revgecoding_df.dropna(subset=['LAT', 'LON', 'ZIPcode'])
'''
#we chech if there are multiple zip codes for a pair of LAT, LON values
zipcode_counts = revgecoding_df.groupBy('LAT', 'LON') \
    .agg(countDistinct('ZIPCode').alias('num_zipcodes')) \
    .filter(col('num_zipcodes') > 1) \
    .count()
print("Num of instances with multiple zipcodes: ", zipcode_counts)
'''
#we drop rows that are irrelevant (null or not in 2015)
crime_df_na_vDesc=crime_df.na.drop(subset=["Vict Descent"])
crime_df_na_vDesc_2015 = crime_df_na_vDesc.filter(year('DATE OCC') == 2015)
crime_df_trunc = crime_df_na_vDesc_2015.select("DR_NO", "LAT", "LON", "Vict Descent")

crimes_zip_joined=crime_df_trunc.hint("SHUFFLE_REPLICATE_NL").join(revgecoding_df, ['LAT', 'LON'], 'inner')
crimes_zip_joined.explain()

unique_zip_codes = crimes_zip_joined.select('ZIPcode').distinct().rdd.flatMap(lambda x: x).collect()
filtered_income_df = income_df.filter(income_df['Zip code'].isin(unique_zip_codes))

min_income = filtered_income_df.select('Zip Code', 'Estimated Median Income') \
    .orderBy('Estimated Median Income') \
    .limit(3)

max_income = filtered_income_df.select('Zip Code', 'Estimated Median Income') \
    .orderBy(desc('Estimated Median Income')) \
    .limit(3)

#execute the query
min_income.createOrReplaceTempView("min_income_view")
max_income.createOrReplaceTempView("max_income_view")
crimes_zip_joined.createOrReplaceTempView("crimes_zip_joined_view")

query3a = """
    SELECT  CASE `Vict Descent`
                WHEN 'W' THEN 'White'
                WHEN 'B' THEN 'Black'
                WHEN 'H' THEN 'Hispanic'
                WHEN 'A' THEN 'Other Asian'
                WHEN 'C' THEN 'Chinese'
                WHEN 'D' THEN 'Cambodian'
                WHEN 'F' THEN 'Filipino'
                WHEN 'X' THEN 'Unknown'
                WHEN 'G' THEN 'Guamanian'
                WHEN 'I' THEN 'Indian/ Native'
                WHEN 'O' THEN 'Other'
                WHEN 'K' THEN 'Korean'
                ELSE `Vict Descent`
            END AS `Victim Descent`,
            COUNT(*) AS Count
    FROM crimes_zip_joined_view
    WHERE ZIPCode IN (SELECT `Zip Code` FROM min_income_view)
    GROUP BY `Vict Descent`
    ORDER BY Count DESC
"""
result3a=spark.sql(query3a)
result3a.show(result3a.count(), False)

query3b = """
    SELECT CASE `Vict Descent`
                WHEN 'W' THEN 'White'
                WHEN 'B' THEN 'Black'
                WHEN 'H' THEN 'Hispanic'
                WHEN 'A' THEN 'Other Asian'
                WHEN 'C' THEN 'Chinese'
                WHEN 'D' THEN 'Cambodian'
                WHEN 'F' THEN 'Filipino'
                WHEN 'X' THEN 'Unknown'
                WHEN 'G' THEN 'Guamanian'
                WHEN 'I' THEN 'Indian/ Native'
                WHEN 'O' THEN 'Other'
                WHEN 'K' THEN 'Korean'
                ELSE `Vict Descent`
            END AS `Victim Descent`,
            COUNT(*) AS Count
    FROM crimes_zip_joined_view
    WHERE ZIPCode IN (SELECT `Zip Code` FROM max_income_view)
    GROUP BY `Vict Descent`
    ORDER BY Count DESC
"""
result3b=spark.sql(query3b)
result3b.show(result3b.count(), False)

end_time = datetime.datetime.now()
execution_time = end_time - start_time
print("Execution Time:", execution_time)