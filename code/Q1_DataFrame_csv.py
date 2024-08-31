from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col, count, rank, to_date, year, month
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Query 1 DataFrame csv") \
    .getOrCreate()

crimes_schema = StructType([
        StructField("DR_NO", StringType()),
        StructField("Date Rptd", StringType()),
        StructField("DATE OCC", StringType()),
        StructField("TIME OCC", StringType()),
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
        StructField("LAT", FloatType()),
        StructField("LON", FloatType()),
    ])


crimes_df1 = spark.read.format('csv') \
    .options(header=True, inferSchema=False) \
    .schema(crimes_schema) \
    .load("hdfs://master:9000/home/user/project2024/crime_data_2010_2019.csv")

crimes_df2 = spark.read.format('csv') \
    .options(header=True, inferSchema=False) \
    .schema(crimes_schema) \
    .load("hdfs://master:9000/home/user/project2024/crime_data_2020_present.csv")

df = crimes_df1.union(crimes_df2)

df = df.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a")) \
    .withColumn("year", year("DATE OCC")) \
    .withColumn("month", month("DATE OCC"))

window_spec = Window.partitionBy("year").orderBy(col("crime_total").desc())

monthly_crime_counts = df.groupBy("year", "month") \
    .agg(count("*").alias("crime_total")) \
    .withColumn("ranking", rank().over(window_spec)) \
    .filter(col("ranking") <= 3) \
    .orderBy(col("year").asc(), col("ranking").asc())

monthly_crime_counts.show()

data = monthly_crime_counts.collect()

with open("Q1_DataFrame_csv.txt", 'w') as new_file:
    for d in data:
        resdata = ""
        for x in d:
            if type(x) == list or type(x) == tuple:
                for t in x:
                    resdata += str(t) + ", "
            else:
                resdata += str(x) + ", "
        resdata += "\n"
        new_file.write(resdata)

spark.stop()
