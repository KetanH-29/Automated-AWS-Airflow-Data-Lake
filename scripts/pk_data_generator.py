import random
import string
from datetime import datetime
import pymysql
from pymysql import Error
import os

# --- Global Data Storage (for consistency) ---
CUSTOMER_LOOKUP = {}


# --- Table Creation Function (Self-Sufficient Setup) ---
def create_table_if_not_exists(cursor, table_name):
    """
    Creates the table with the transaction schema (cust_id is Primary Key).
    """
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        cust_id INT NOT NULL,
        cust_name VARCHAR(100) NOT NULL,
        transaction_amount DECIMAL(10, 2) NOT NULL,
        transaction_datetime TIMESTAMP NOT NULL,
        PRIMARY KEY (cust_id) 
    );
    """
    cursor.execute(create_table_query)
    print(f"✅ Table '{table_name}' checked/created with PRIMARY KEY (cust_id).")


def generate_random_name(length=8):
    """Generates a random string for a customer name."""
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length)).title()


def generate_and_upsert_accounts(db_conf: dict, table_name, num_rows=2):
    """
    Generates data rows and performs an UPSERT (INSERT or UPDATE) into MySQL.
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

    # 2. Database Insertion (PyMySQL Method with Robust Parsing FIX)
    connection = None
    try:
        # --- FIX: Robust Connection Parameter Extraction ---
        # The URL is structured as: jdbc:mysql://HOST:PORT/DB?params
        full_url_block = db_conf["JDBC_URL"].split("//")[1]

        # 1. Host (e.g., 'localhost') and Port (3307)
        host = full_url_block.split(":")[0]
        port = int(full_url_block.split(":")[1].split("/")[0])

        # 2. Database Name Extraction (Pulls ONLY 'testdb')
        # This resolves the Access Denied issue by passing only 'testdb'.
        database_section = full_url_block.split(f":{port}/")[-1]
        database = database_section.split("?")[0]

        # --- PyMySQL Connection ---
        connection = pymysql.connect(
            host=host,
            port=port,
            database=database,  # THIS IS NOW GUARANTEED TO BE ONLY 'testdb'
            user=db_conf["USERNAME"],
            password=db_conf["PASSWORD"],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        if connection.open:
            cursor = connection.cursor()

            # --- CREATE TABLE FIRST ---
            create_table_if_not_exists(cursor, table_name)

            # --- UPSERT SQL ---
            upsert_query = f"""
            INSERT INTO {table_name} 
            (cust_id, cust_name, transaction_amount, transaction_datetime)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                cust_name = VALUES(cust_name),
                transaction_amount = VALUES(transaction_amount),
                transaction_datetime = VALUES(transaction_datetime)
            """

            cursor.executemany(upsert_query, new_rows_data)
            connection.commit()

            print(f"✅ Successfully inserted/updated {num_rows} new rows via PyMySQL connector.")

    except Error as e:
        print(f"❌ Database Insertion Failed: {e}")
        raise  # Reraise the error to the main script

    finally:
        if connection and connection.open:
            connection.close()