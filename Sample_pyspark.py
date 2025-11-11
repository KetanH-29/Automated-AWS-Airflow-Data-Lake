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
    .appName("Test")
    .master("local[*]")
    .config("spark.jars", "file:///C:/spark/jars/mysql-connector-j-9.5.0.jar")
    .getOrCreate()
)


try:
    df = (
        spark.read
        .format("jdbc")
        .option("url", mysql["JDBC_URL"])
        .option("dbtable", mysql["TABLE_NAME"])
        .option("user", mysql["USERNAME"])
        .option("password", mysql["PASSWORD"])
        .option("driver", mysql["JDBC_DRIVER"])
        .load()
    )
    print("Data Read Successfully✅")
    df.printSchema()
    df.show()

    df.write.mode("overwrite").parquet(mysql["PARQUET_OUTPUT_PATH"])

    print(f"\n--------- SUCCESS ✅-----------")

except Exception as e:
    print(f"\n-------------- EXECUTION FAILED ✅--------------")
    print(e)