from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'ketan',
    'start_date': datetime(2024, 1, 1),
    'retries': 1
}

with DAG(
    dag_id='spark_mysql_to_parquet',
    default_args=default_args,
    description='Run Spark ETL pipeline: MySQL → Spark → Parquet',
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'mysql', 'parquet']
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "🚀 Starting the ETL pipeline..."'
    )

    run_spark_etl = BashOperator(
        task_id='run_spark_etl',
        bash_command=(
            '/home/sparkuser/spark/bin/spark-submit '
            '--master local[*] '
            '--jars /opt/airflow/jars/mysql-connector-j-9.5.0.jar '
            '/opt/airflow/scripts/New_updated.py {{ dag_run.conf.get("table_name", "customers") }}'
        )
    )

    end = BashOperator(
        task_id='end',
        bash_command='echo "✅ ETL pipeline completed successfully!"'
    )

    start >> run_spark_etl >> end
