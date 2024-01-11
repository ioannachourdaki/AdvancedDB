from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType
from pyspark.sql.functions import col, to_date
import datetime

#Imports for query1
from pyspark.sql.functions import month, year, count, desc, rank, countDistinct, regexp_replace
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Dataframe") \
    .getOrCreate()

# Set the legacy time parser policy
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

LA_Crime_Data = StructType([
    StructField("DR_NO", IntegerType()),
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("TIME OCC", IntegerType()),
    StructField("AREA", IntegerType()),
    StructField("AREA NAME", StringType()),
    StructField("Rpt Dist No", IntegerType()),
    StructField("Part 1-2", IntegerType()),
    StructField("Crm Cd", IntegerType()),
    StructField("Crm Cd Desc", StringType()),
    StructField("Mocodes", StringType()),
    StructField("Vict Age", IntegerType()),
    StructField("Vict Sex", StringType()),
    StructField("Vict Descent", StringType()),
    StructField("Premis Cd", IntegerType()),
    StructField("Premis Desc", StringType()),
    StructField("Weapon Used Cd", IntegerType()),
    StructField("Weapon Desc", StringType()),
    StructField("Status", StringType()),
    StructField("Status Desc", StringType()),
    StructField("Crm Cd 1", IntegerType()),
    StructField("Crm Cd 2", IntegerType()),
    StructField("Crm Cd 3", IntegerType()),
    StructField("Crm Cd 4", IntegerType()),
    StructField("LOCATION", StringType()),
    StructField("Cross Street", StringType()),
    StructField("LAT", DoubleType()),
    StructField("LON", DoubleType())
])

crime_df1 = spark.read.csv("hdfs:///user/user/Crime_Data_from_2010_to_2019.csv", header=True, schema=LA_Crime_Data)
print("Total number of rows df1:", crime_df1.count())

crime_df2 = spark.read.csv("hdfs:///user/user/Crime_Data_from_2020_to_Present.csv", header=True, schema=LA_Crime_Data)
print("Total number of rows df2:", crime_df2.count())

# Concatenate DataFrames
crime_df = crime_df1.union(crime_df2)
crime_df=crime_df.withColumn("Date Rptd", to_date(col("Date Rptd"), "MM/dd/yyyy"))
crime_df=crime_df.withColumn("DATE OCC", to_date(col("DATE OCC"), "MM/dd/yyyy"))
crime_df = crime_df.dropDuplicates()

# Print the total number of rows
print("Total number of rows df:", crime_df.count())
# Prin data type of each column
print("Schema:")
crime_df.printSchema()

crime_df.write.parquet("output/crime_df.parquet")