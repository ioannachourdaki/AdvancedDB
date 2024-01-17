from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, TimestampType
from pyspark.sql.functions import col, to_date, when, substring
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
import math
from pyspark.sql import SparkSession
import datetime

# Function for diastance calculation
def get_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Differences in coordinates
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Haversine formula
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c  # Distance in kilometers

    return distance


# Create Spark session
spark = SparkSession.builder.appName("Query 4").getOrCreate()
# Register the UDF
spark.udf.register("get_distance", get_distance, DoubleType())

crime_df = spark.read.parquet("output/crime_df.parquet")

# Precincts data
precincts_schema = StructType([
    StructField("X", DoubleType()),
    StructField("Y", DoubleType()),
    StructField("FID", IntegerType()),
    StructField("DIVISION", StringType()),
    StructField("LOCATION", StringType()),
    StructField("PREC", IntegerType())
])

Precincts_df = spark.read.csv("LAPD_Police_Stations.csv", header=True, schema=precincts_schema)

# Create Tables
crime_df.createOrReplaceTempView("CrimeReports")
Precincts_df.createOrReplaceTempView("Precincts")

# Create useful views

view1  = """
CREATE OR REPLACE TEMPORARY VIEW CrimeView1 AS
SELECT * FROM CrimeReports
WHERE LAT <> 0.0 AND LON <> 0.0 AND `Weapon Used Cd` BETWEEN 100 AND 199
"""
view2  = """
CREATE OR REPLACE TEMPORARY VIEW CrimeView2 AS
SELECT * FROM CrimeReports
WHERE LAT <> 0.0 AND LON <> 0.0 AND `Weapon Used Cd` IS NOT NULL
"""

spark.sql(view1)
spark.sql(view2)

start_time = datetime.datetime.now()

#spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
#spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")

### Query 4a (1) ###

query4a1 = """
SELECT YEAR(c.`DATE OCC`) AS Year, AVG(get_distance(c.LAT, c.LON, p.Y, p.X)) AS `Average Distance`, COUNT(*) AS Count
FROM CrimeView1 AS c
INNER JOIN (SELECT /*+ BROADCAST(p) */ * FROM Precincts p) p ON c.AREA = p.PREC
GROUP BY Year
ORDER BY Year
"""

#INNER JOIN (SELECT /*+ BROADCAST(p) */ * FROM Precincts p) p ON c.AREA = p.PREC

query4a1_df = spark.sql(query4a1)
query4a1_df.show()

### Query 4b (1) ###

query4b1 = """
SELECT p.DIVISION AS Division, AVG(get_distance(c.LAT, c.LON, p.Y, p.X)) AS `Average Distance`, COUNT(*) AS Count
FROM CrimeView2 AS c
INNER JOIN (SELECT /*+ BROADCAST(p) */ * FROM Precincts p) p ON c.AREA = p.PREC
GROUP BY Division
ORDER BY Count DESC;
"""

query4b1_df = spark.sql(query4b1)
query4b1_df.show()

query4b1_df.explain()


end_time = datetime.datetime.now()
execution_time=end_time-start_time
print("Total Execution Time:", execution_time)
