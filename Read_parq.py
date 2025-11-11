import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import yaml

with open("paths.yml", "r") as R:
    conf = yaml.safe_load(R)

mysql = conf["mysql"]

spark = (
    SparkSession.builder
    .appName("Read")
    .master("local[*]")
    .config("spark.jars", "file:///C:/spark/jars/mysql-connector-j-9.5.0.jar")
    .getOrCreate()
)


parquet_df = spark\
            .read\
            .parquet(r"C:\Users\hadka\OneDrive\Desktop\Sparking_Flow\mirror\accounts")

parquet_df.printSchema()
print(parquet_df.count())