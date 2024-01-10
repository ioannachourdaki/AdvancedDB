from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, TimestampType
from pyspark.sql.functions import col, to_date, when, substring
from pyspark.sql import functions as F
from io import StringIO
import csv
import time


### DataFrame/SQL API ###

spark = SparkSession.builder \
    .appName("Query 2 using SQL") \
    .config("spark.executor.instances", 4) \
    .getOrCreate()

# Set the legacy time parser policy
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

crime_df = spark.read.parquet("output/crime_df.parquet")

crime_df.createOrReplaceTempView("CrimeReports")

query2 = """
SELECT
  CASE
    WHEN `TIME OCC` BETWEEN 0500 AND 1159 THEN 'Morning'
    WHEN `TIME OCC` BETWEEN 1200 AND 1659 THEN 'Afternoon'
    WHEN `TIME OCC` BETWEEN 1700 AND 2059 THEN 'Evening'
    WHEN `TIME OCC` BETWEEN 0000 AND 0459 OR `TIME OCC` BETWEEN 2100 AND 2359 THEN 'Night'
  END AS `Day Segments`, COUNT(*) AS Count
FROM CrimeReports
WHERE `Premis Desc` = 'STREET'
GROUP BY `Day Segments`
ORDER BY Count DESC
"""

# Start timer
start_time = time.time()
# Execute Query 2
query2_df = spark.sql(query2)
query2_df.show()
# Stop timer
end_time = time.time()
# Calculate the runtime
print(f"DataFrame/SQL API runtime: {end_time - start_time} seconds")
# Stop the Spark session when done
spark.stop()


### RDD API ###

spark = SparkSession.builder \
    .appName("Query 2 using RDD") \
    .config("spark.executor.instances", 4) \
    .getOrCreate()

# Get the SparkContext
sc = spark.sparkContext

# Open 1st file
data1 = sc.textFile("hdfs:///user/user/Crime_Data_from_2010_to_2019.csv").map(lambda x: next(csv.reader(StringIO(x))))
headers = data1.first()
data1 = data1.filter(lambda x: x != headers)

# Open 2nd file
data2 = sc.textFile("hdfs:///user/user/Crime_Data_from_2020_to_Present.csv").map(lambda x: next(csv.reader(StringIO(x))))
headers = data2.first()
data2 = data2.filter(lambda x: x != headers)

# Merge the 2 files
data = data1.union(data2)
data = data.map(lambda x: [element.split(" ")[0] if (i == 1 or i == 2) else element for i, element in enumerate(x)])
headers = data.first()
data = data.filter(lambda x: x != headers)

# Delete duplicates
data = data.map(lambda x: (x[0], x))
data = data.groupByKey().map(lambda x: (x[0], list(x[1])[0]))
data = data.map(lambda x: x[1])

# Start timer
start_time = time.time()

# Find STREET
Premis_Street = data.map(lambda x: x if (x[15] == "STREET") else None).filter(lambda x: x != None)

# Separate the day in 4 segments
Morning = Premis_Street.map(lambda x: x if ( (int(x[3]) >= 500) and (int(x[3]) <= 1159) ) else None).filter(lambda x: x != None)
Afternoon = Premis_Street.map(lambda x: x if ( (int(x[3]) >= 1200) and (int(x[3]) <= 1659) ) else None).filter(lambda x: x != None)
Evening = Premis_Street.map(lambda x: x if ( (int(x[3]) >= 1700) and (int(x[3]) <= 2059) ) else None).filter(lambda x: x != None)
Night = Premis_Street.map(lambda x: x if ( ( (int(x[3]) >= 0) and (int(x[3]) <= 459) ) or ( (int(x[3]) >= 2100) and (int(x[3]) <= 2359) ) ) else None).filter(lambda x: x != None)

# Create a list of tuples containing the Day Segment and its count
counts = [("Morning", Morning.count()),
          ("Afternoon", Afternoon.count()),
          ("Evening", Evening.count()),
          ("Night", Night.count())]

# Sort the list of tuples by count in descending order
sorted_counts = sorted(counts, key=lambda x: x[1], reverse=True)
print(sorted_counts)

# Stop timer
end_time = time.time()
# Calculate the runtime
print(f"RDD API runtime: {end_time - start_time} seconds")

# Stop the Spark session when done
spark.stop()
