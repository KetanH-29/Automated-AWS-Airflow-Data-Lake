import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from data_generator import generate_rows_and_insert
from pk_data_generator import generate_and_upsert_accounts
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

def read_from_table(spark, mysql, query):
    print(query)
    df = (
        spark.read \
            .format("jdbc") \
            .option("url", mysql["JDBC_URL"]) \
            .option("user", mysql["USERNAME"]) \
            .option("password", mysql["PASSWORD"]) \
            .option("driver", mysql["JDBC_DRIVER"]) \
            .option("query", query)
            .load()
    )
    return df


def read_from_db(table_name):
    try:
        if table_name in mysql["TABLES"]:

            load_type = mysql["TABLES"][table_name]["load_type"]
            column_name = mysql["TABLES"][table_name]["columns"]
            if load_type == "FULL":
                query = f"select {column_name} from {table_name}"

                df = read_from_table(spark, mysql, query)

                print("Data Read Successfully✅")
                df.printSchema()
                df.show()

                OUTPUT_PATH = f"{mysql['LANDING_PATH']}/{table_name}"
                df.write.mode("overwrite").parquet(OUTPUT_PATH)

                print(f"\n--------- SUCCESS ✅-----------")
                print(f"Data Written to : {OUTPUT_PATH}")

        else:
            print("Table not present in database")


    except Exception as e:
        print("ERROR OCCURRED❌")
        print(e)


def landing_to_mirror(table_name):
    try:
        if table_name in mysql["TABLES"]:
            load_type = mysql["TABLES"][table_name]["load_type"]
            column_name = mysql["TABLES"][table_name]["columns"]
            if load_type == "FULL":
                LANDING_PATH = f"{mysql['LANDING_PATH']}/{table_name}"
                MIRROR_PATH = f"{mysql['MIRROR_PATH']}/{table_name}"
                df = spark.read.format("parquet").load(f"{LANDING_PATH}")

                df.write.mode("overwrite").parquet(f"{MIRROR_PATH}")

        if table_name in mysql["TABLES"]:
            load_type = mysql["TABLES"][table_name]["load_type"]
            column_name = mysql["TABLES"][table_name]["columns"]
            incremental_col = mysql["TABLES"][table_name]["incremental_col"]
            pk = mysql["TABLES"][table_name]["primary_key"]
            if load_type == "INCREMENTAL" and pk == "None":
                generate_rows_and_insert(mysql, table_name, num_rows=2)
                LANDING_PATH = f"{mysql['LANDING_PATH']}/{table_name}"
                MIRROR_PATH = f"{mysql['MIRROR_PATH']}/{table_name}"
                try:
                    df = spark.read.format("parquet").load(f"{MIRROR_PATH}")
                    max_value = df.agg(max(col("transaction_datetime"))).collect()[0][0]
                    print(max_value)
                except Exception as e:
                    print(e)
                    out = "does not exist"
                    if out in str(e):
                        max_value = None
                    else:
                        raise e
                if max_value is None:
                    query = f"select {column_name} from {table_name}"
                else:
                    query = f"select {column_name} from {table_name} where {incremental_col} > '{max_value}'"

                df = read_from_table(spark, mysql, query)
                df.write.mode("append").parquet(f"{LANDING_PATH}")

                df = spark.read.format("parquet").load(f"{LANDING_PATH}")
                df.write.mode("overwrite").parquet(f"{MIRROR_PATH}")

            elif load_type == "INCREMENTAL" and pk != "None":
                LANDING_PATH = f"{mysql['LANDING_PATH']}/{table_name}"
                MIRROR_PATH = f"{mysql['MIRROR_PATH']}/{table_name}"
                TEMP_PATH = f"{mysql['TEMP_PATH']}/{table_name}"
                try:
                    try:
                        df = spark.read.format("parquet").load(f"{MIRROR_PATH}")
                        df.write.mode("overwrite").parquet(f"{TEMP_PATH}")
                        print(f"MIRROR Data Written to : {TEMP_PATH} Successfully✅")
                        df.show(n=1000, truncate=False)
                        print("PRINTING TEMP COUNT")
                        print(df.count())
                    except Exception as e:
                        print(e)
                        out = "does not exist"
                        if out in str(e):
                            df = spark.read.format("parquet").load(f"{LANDING_PATH}")
                            df.write.mode("overwrite").parquet(f"{MIRROR_PATH}")
                            df.show(n=1000, truncate=False)
                            print("PRINTING MIRROR count")
                            print(df.count())
                            print(f"MIRROR Data Written to : {MIRROR_PATH} Successfully✅")
                        else:
                            print(f"Data Not Present at{MIRROR_PATH}")
                    df_new = spark.read.format("parquet").load(f"{LANDING_PATH}")
                    df_old = spark.read.format("parquet").load(f"{TEMP_PATH}")
                    df_unchanged = df_old.join(df_new, on="cust_id", how="left_anti")
                    df_unchanged.show(n=1000, truncate=False)
                    print("PRINTING UNCHANGED count")
                    print(df_unchanged.count())
                    df_new.show(n=1000, truncate=False)
                    print("PRINTING NEW count")
                    print(df_new.count())
                    df_merged = df_unchanged.unionByName(df_new)
                    df_merged.show(n=1000, truncate=False)
                    print("PRINTING MERGED count")
                    print(df_merged.count())
                    df_merged.write.mode("overwrite").parquet(f"{MIRROR_PATH}")
                    print(f"MERGED Data Written to : {MIRROR_PATH} Successfully✅")
                    print("Data Present in Temp✅")
                except Exception as e:
                    print(e)
    except Exception as e:
        print("ERROR")
        print(e)

table_name = input("Enter Table Name: ")

generate_rows_and_insert(mysql, table_name, num_rows=2)

read_from_db(table_name)
landing_to_mirror(table_name)
