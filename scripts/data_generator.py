import random
import string
from datetime import datetime
import pymysql
from pymysql import Error
import os
import sys

# Setting the timezone early is crucial for resolving crashes related to system time
os.environ['TZ'] = 'Asia/Kolkata'

# --- Global Data Storage (for consistency) ---
CUSTOMER_LOOKUP = {}


def generate_random_name(length=8):
    """Generates a random string for a customer name."""
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length)).title()


def generate_rows_and_insert(db_conf: dict, table_name, num_rows=2):
    """
    Generates data rows and inserts them directly into MySQL using the PyMySQL connector.
    """
    new_rows_data = []

    # 1. Generate Rows
    for _ in range(num_rows):
        cust_id = random.randint(1, 100)

        if cust_id not in CUSTOMER_LOOKUP:
            CUSTOMER_LOOKUP[cust_id] = generate_random_name()

        cust_name = CUSTOMER_LOOKUP[cust_id]
        transaction_amount = round(random.uniform(1, 1000), 2)
        transaction_datetime = datetime.now()

        new_rows_data.append((cust_id, cust_name, transaction_amount, transaction_datetime))

    # 2. Database Insertion (PyMySQL Method with Robust Parsing)
    connection = None
    try:
        # --- FIX: Robust Connection Parameter Extraction ---
        full_url_block = db_conf["JDBC_URL"].split("//")[1]

        # 1. Host Extraction (e.g., 'localhost')
        host = full_url_block.split(":")[0]

        # 2. Port Extraction (e.g., 3307)
        port_section = full_url_block.split(":")[1]
        port = int(port_section.split("/")[0])

        # 3. Database Name Extraction (Ignores everything after '?')
        # Finds the string after the port/ and before the '?'
        database_section = full_url_block.split(f":{port}/")[-1]
        database = database_section.split("?")[0]  # Extracts ONLY 'testdb'

        # --- PyMySQL Connection ---
        connection = pymysql.connect(
            host=host,
            port=port,
            database=database,
            user=db_conf["USERNAME"],
            password=db_conf["PASSWORD"],

            # Required parameters for PyMySQL
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        if connection.open:
            cursor = connection.cursor()

            insert_query = f"""
            INSERT INTO {table_name} 
            (cust_id, cust_name, transaction_amount, transaction_datetime)
            VALUES (%s, %s, %s, %s)
            """

            cursor.executemany(insert_query, new_rows_data)
            connection.commit()

            print(f"✅ Successfully inserted {num_rows} new rows via PyMySQL connector.")

    except Error as e:
        print(f"❌ Database Insertion Failed: {e}")
        raise

    finally:
        if connection and connection.open:
            connection.close()