from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType, DateType
from pyspark.sql.functions import col, count, month, year, rank, substring, when, hour, regexp_replace, to_date
import sys

spark = SparkSession \
    .builder \
    .appName("Query 3 DataFrame optimizer") \
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

income_2015_schema = StructType([
    StructField("Zip Code", StringType()),
    StructField("Community", StringType()),
    StructField("Estimated Median Income", StringType())
])

geocoding_schema = StructType([
    StructField("LAT", FloatType()),
    StructField("LON", FloatType()),
    StructField("ZIPcode", StringType())
])

crimes_df = spark.read.format('csv') \
    .options(header=True, inferSchema=False) \
    .schema(crimes_schema) \
    .load("hdfs://master:9000/home/user/project2024/crime_data_2010_2019.csv")

income_df = spark.read.format('csv') \
    .options(header=True, inferSchema=False) \
    .schema(income_2015_schema) \
    .load("hdfs://master:9000/home/user/project2024/LA_income_2015.csv")

geocoding_df = spark.read.format('csv') \
    .options(header=True, inferSchema=False) \
    .schema(geocoding_schema) \
    .load("hdfs://master:9000/home/user/project2024/revgecoding.csv")

crimes_df = crimes_df.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a"))

income_df = income_df.withColumn(
    "Estimated Median Income",
    regexp_replace(col("Estimated Median Income"), "[$,]", "").cast("int")
)

crimes_2015_df = crimes_df.filter((year(col("DATE OCC")) == 2015) &
                                   (col("Vict Descent").isin("A", "B", "C", "D", "F", "G", "H", "I", "J", "K", "L", "O", "P", "S", "U", "V", "W", "X", "Z")))

geocoding_df = geocoding_df.withColumn("code", substring(col("ZIPcode"), 1, 5))


def optimize_joins(method):
    global crimes_2015_df, geocoding_df, income_df
    geocoding_df = geocoding_df.hint(method)
    income_df = income_df.hint(method)
    crimes_2015_df = crimes_2015_df.hint(method)

    joined_df = crimes_2015_df.join(geocoding_df, on=["LAT", "LON"], how="inner") \
        .select("LAT", "LON", "DATE OCC", "Vict Descent", "code") \
        .withColumn("Vict Descent",
                    when(col("Vict Descent") == "A", "Other Asian")
                    .when(col("Vict Descent") == "B", "Black")
                    .when(col("Vict Descent") == "C", "Chinese")
                    .when(col("Vict Descent") == "D", "Cambodian")
                    .when(col("Vict Descent") == "F", "Filipino")
                    .when(col("Vict Descent") == "G", "Guamanian")
                    .when(col("Vict Descent") == "H", "Hispanic/Latin/Mexican")
                    .when(col("Vict Descent") == "I", "American Indian/Alaskan Native")
                    .when(col("Vict Descent") == "J", "Japanese")
                    .when(col("Vict Descent") == "K", "Korean")
                    .when(col("Vict Descent") == "L", "Laotian")
                    .when(col("Vict Descent") == "O", "Other")
                    .when(col("Vict Descent") == "P", "Pacific Islander")
                    .when(col("Vict Descent") == "S", "Samoan")
                    .when(col("Vict Descent") == "U", "Hawaiian")
                    .when(col("Vict Descent") == "V", "Vietnamese")
                    .when(col("Vict Descent") == "W", "White")
                    .when(col("Vict Descent") == "Z", "Asian Indian")
                    .when(col("Vict Descent") == "X", "Unknown")
                    ).hint(method)

    top_3_income = income_df.join(joined_df, joined_df["code"] == income_df['Zip Code'], how="left_semi") \
        .orderBy(col("Estimated Median Income").desc()).limit(3).select("Zip Code").hint(method)

    bottom_3_income = income_df.join(joined_df, joined_df["code"] == income_df['Zip Code'], how="left_semi") \
        .orderBy(col("Estimated Median Income").asc()).limit(3).select("Zip Code").hint(method)

    total_victims_top_3 = joined_df.join(top_3_income, joined_df["code"] == top_3_income['Zip Code'], how="inner") \
        .groupBy("Vict Descent").agg(count("*").alias("total_victims")).orderBy(col("total_victims").desc())

    total_victims_bottom_3 = joined_df.join(bottom_3_income, joined_df["code"] == bottom_3_income['Zip Code'], how="inner") \
        .groupBy("Vict Descent").agg(count("*").alias("total_victims")).orderBy(col("total_victims").desc())

    print(total_victims_top_3.show())
    print(total_victims_bottom_3.show())

    together = total_victims_top_3.union(total_victims_bottom_3)
    data = together.collect()

    with open("Q3_Dataframe_" + str(method) + ".txt", 'w') as new_file:
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

    # physical plans
    # joined_df.explain()
    # top_3_income.explain()
    # bottom_3_income.explain()
    # total_victims_top_3.explain()
    # total_victims_bottom_3.explain()


# BROADCAST, MERGE, SHUFFLE_HASH,SHUFFLE_REPLICATE_NL
method = sys.argv[1]
optimize_joins(method)

spark.stop()
