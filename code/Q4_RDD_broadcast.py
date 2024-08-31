from pyspark.sql import SparkSession
import csv
import io
import math

spark = SparkSession \
    .builder \
    .appName("Q4_RDD_broadcast") \
    .getOrCreate() \
    .sparkContext


def custom_csv_split(line):
    reader = csv.reader(io.StringIO(line), delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    return next(reader)


crimes1 = spark.textFile("hdfs://master:9000/home/user/project2024/crime_data_2010_2019.csv") \
    .map(custom_csv_split)

crimes2 = spark.textFile("hdfs://master:9000/home/user/project2024/crime_data_2020_present.csv") \
    .map(custom_csv_split)

LAPD_rdd = spark.textFile("hdfs://master:9000/home/user/project2024/LAPD_Police_Stations_new.csv") \
    .map(lambda x: (x.split(",")))

header_LAPD = LAPD_rdd.first()
LAPD_rdd = LAPD_rdd.filter(lambda line: line != header_LAPD)

crimes_rdd = crimes1.union(crimes2)

crimes_rdd_formatted = crimes_rdd.filter(lambda x: x[26] != "0" and x[27] != "0" and len(x[16]) > 0 and x[16][0] == "1") \
                                 .map(lambda x: [int(x[4]), [x[16], x[26], x[27]]])

LAPD_rdd_formatted = LAPD_rdd.map(lambda x: [int(x[3]), [x[1], x[4], x[5]]])

broadcasted_LAPD_rdd = spark.broadcast(LAPD_rdd_formatted.keyBy(lambda x: x[0]).collectAsMap()) # dictionary

broadcast_value = broadcasted_LAPD_rdd.value

joined_rdd = crimes_rdd_formatted.map(lambda x: [x[0], [x[1], broadcast_value.get(x[0])]])


def get_distance(lat1, long1, lat2, long2):
    lat1, long1, lat2, long2 = map(float, [lat1, long1, lat2, long2])
    lat1, long1, lat2, long2 = map(math.radians, [lat1, long1, lat2, long2])

    dlat = lat2 - lat1
    dlon = long2 - long1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371.0
    return c * r


joined_rdd = joined_rdd.map(lambda x: (
      x[0],
      x[1][0],
      x[1][1],
      get_distance(x[1][0][1], x[1][0][2], x[1][1][1][2], x[1][1][1][1]))) \
      .map(lambda x: ([x[2][1][0], (x[3], 1)])) \
      .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
      .mapValues(lambda x: (round(x[0] / x[1], 3), x[1])) \
      .sortBy(lambda x: x[1][1], ascending=False)

print(joined_rdd.take(21))
data = joined_rdd.collect()

with open("Q4_RDD_broadcast.txt", 'w') as new_file:
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
