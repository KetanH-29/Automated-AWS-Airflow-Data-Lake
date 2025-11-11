import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from data_generator import generate_rows_and_insert
from pk_data_generator import generate_and_upsert_accounts
from Elasticsearch import log_job_status
import yaml


with open("paths.yml", "r") as R:
    conf = yaml.safe_load(R)

mysql = conf["mysql"]


SPARK_JARS = (
                "file:///C:/spark/jars/mysql-connector-j-9.5.0.jar"            )

spark = (
    SparkSession.builder
    .appName("Test")
    .master("local[*]")
    .config("spark.jars", SPARK_JARS)
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
        log_job_status("MySQL", table_name, "READ_DB_WRITE_LANDING", "STARTED", "Reading data from database")
        if table_name in mysql["TABLES"]:
            load_type = mysql["TABLES"][table_name]["load_type"]
            column_name = mysql["TABLES"][table_name]["columns"]
            incremental_col = mysql["TABLES"][table_name]["incremental_col"]
            pk = mysql["TABLES"][table_name]["primary_key"]
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

            elif load_type == "INCREMENTAL" and pk == "None":
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
                    write_mode = "overwrite"
                else:
                    query = f"select {column_name} from {table_name} where {incremental_col} > '{max_value}'"
                    write_mode = "overwrite"

                df = read_from_table(spark, mysql, query)
                df.write.mode(write_mode).parquet(f"{LANDING_PATH}")

                print(f"Data Written to : {LANDING_PATH} with mode {write_mode} Successfully✅")

            elif load_type == "INCREMENTAL" and pk != "None":
                generate_and_upsert_accounts(mysql, table_name, num_rows=2)
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
                    write_mode = "overwrite"
                else:
                    query = f"select {column_name} from {table_name} where {incremental_col} > '{max_value}'"
                    write_mode = "overwrite"

                df = read_from_table(spark, mysql, query)
                df.write.mode(write_mode).parquet(f"{LANDING_PATH}")
                df.show(n=1000, truncate=False)
                print("PRINTING LANDING COUNT")
                print(df.count())

                print(f"Data Written to : {LANDING_PATH} with mode {write_mode} Successfully✅")


        else:
            log_job_status("MySQL", table_name, "READ_DB_WRITE_LANDING", "FAILED", "Table not present in database")
            print("Table not present in database")
        log_job_status("MySQL", table_name, "READ_DB_WRITE_LANDING", "COMPLETED", "Completed processing table")

    except Exception as e:
        log_job_status("MySQL", table_name, "READ_DB_WRITE_LANDING", "FAILED", str(e))
        print("ERROR OCCURRED❌")
        print(e)


def landing_to_mirror(table_name):
    try:
        log_job_status("MySQL", table_name, "LANDING_TO_MIRROR", "STARTED", "Starting landing to mirror load")
        if table_name in mysql["TABLES"]:
            load_type = mysql["TABLES"][table_name]["load_type"]
            column_name = mysql["TABLES"][table_name]["columns"]
            incremental_col = mysql["TABLES"][table_name]["incremental_col"]
            pk = mysql["TABLES"][table_name]["primary_key"]
            if load_type == "FULL":
                LANDING_PATH = f"{mysql['LANDING_PATH']}/{table_name}"
                MIRROR_PATH = f"{mysql['MIRROR_PATH']}/{table_name}"

                df = spark.read.format("parquet").load(f"{LANDING_PATH}")
                df.write.mode("overwrite").parquet(f"{MIRROR_PATH}")
                print(f"LANDING Data Written to : {MIRROR_PATH} Successfully✅")


            elif load_type == "INCREMENTAL" and pk == "None":
                LANDING_PATH = f"{mysql['LANDING_PATH']}/{table_name}"
                MIRROR_PATH = f"{mysql['MIRROR_PATH']}/{table_name}"

                df = spark.read.format("parquet").load(f"{LANDING_PATH}")
                df.write.mode("overwrite").parquet(f"{MIRROR_PATH}")

                print(f"LANDING Data Written to : {MIRROR_PATH} Successfully✅")

            elif load_type == "INCREMENTAL" and pk != "None":
                LANDING_PATH = f"{mysql['LANDING_PATH']}/{table_name}"
                MIRROR_PATH = f"{mysql['MIRROR_PATH']}/{table_name}"
                TEMP_PATH = f"{mysql['TEMP_PATH']}/{table_name}"
                PK_COLUMN = mysql["TABLES"][table_name]["primary_key"]

                is_first_run = False
                try:
                    df = spark.read.format("parquet").load(f"{MIRROR_PATH}")
                    df.write.mode("overwrite").parquet(f"{TEMP_PATH}")
                    print(f"MIRROR Data Written to : {TEMP_PATH} Successfully✅")
                    df.show(n=1000, truncate=False)
                    print("PRINTING TEMP COUNT")
                    print(df.count())
                except Exception as e:
                    out = "does not exist"
                    if out in str(e):
                        is_first_run = True
                        print("Mirror path not found. Preparing initial snapshot.")
                    else:
                        raise e

                if is_first_run:
                    df = spark.read.format("parquet").load(f"{LANDING_PATH}")
                    df.write.mode("overwrite").parquet(f"{MIRROR_PATH}")
                    print(f"✅ UPSERT Load: Initial snapshot copied to Mirror.")
                else:
                    df_old = spark.read.format("parquet").load(f"{TEMP_PATH}")
                    df_new = spark.read.format("parquet").load(f"{LANDING_PATH}")

                    df_unchanged = df_old.join(df_new, on=[PK_COLUMN], how="left_anti")
                    print(f"🔄 Found {df_unchanged.count()} unchanged records.")

                    df_merged = df_unchanged.unionByName(df_new)

                    df_merged.write.mode("overwrite").parquet(f"{MIRROR_PATH}")
                    print(f"✅ Data Merged and Mirror Overwritten.")

            log_job_status("MySQL", table_name, "LANDING_TO_MIRROR", "COMPLETED", "Landing to mirror step completed successfully")

        else:
            log_job_status("MySQL", table_name, "LANDING_TO_MIRROR", "FAILED", "Table not present in database")
            print("Table not present in database")

    except Exception as e:
        log_job_status("MySQL", table_name, "LANDING_TO_MIRROR", "FAILED", str(e))
        print("ERROR")
        print(e)


table_name = input("Enter Table Name: ")

read_from_db(table_name)
landing_to_mirror(table_name)

print("--------------------Pipeline Executed Successfully ✅-----------------------")