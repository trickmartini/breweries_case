from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

bronze_api_url = "https://api.openbrewerydb.org/breweries"
bronze_raw_files = "/opt/airflow/output/bronze/raw/"
bronze_table_path = "/opt/airflow/output/bronze/delta/breweries"
silver_table_path = "/opt/airflow/output/silver/breweries"
gold_table_path = "/opt/airflow/output/gold/breweries_by_location"

dag = DAG(
    dag_id='breweries_etl_process',
    default_args={
        'owner': 'airflow',
        "start_date": datetime(2025, 3, 1),
    },
    schedule_interval='@daily',
)

start = PythonOperator(
    task_id='start',
    python_callable=lambda: print("Jobs Started"),
    dag=dag,
)
bronze_step = SparkSubmitOperator(
    task_id='bronze_step',
    conn_id='spark-conn',
    application="scripts/bronze_process.py",
    conf={
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    },
    packages="io.delta:delta-spark_2.12:3.3.0",
    application_args=[
        "--bronze_raw_files", bronze_raw_files,
        "--bronze_table_path", bronze_table_path,
        "--api_url", bronze_api_url
    ],
    dag=dag
)

silver_step = SparkSubmitOperator(
    task_id='silver_step',
    conn_id='spark-conn',
    application="scripts/silver_process.py",
    conf={
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    },
    packages="io.delta:delta-spark_2.12:3.3.0",
    application_args=[
        "--bronze_table_path", bronze_table_path,
        "--silver_table_path", silver_table_path
    ],
    dag=dag
)
gold_step = SparkSubmitOperator(
    task_id='gold_step',
    conn_id='spark-conn',
    application="scripts/gold_process.py",
    conf={
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    },
    packages="io.delta:delta-spark_2.12:3.3.0",
    application_args=[
        "--gold_table_path", gold_table_path,
        "--silver_table_path", silver_table_path
    ],
    dag=dag
)
# Validation processes
silver_step_validation = SparkSubmitOperator(
    task_id='silver_step_validation',
    conn_id='spark-conn',
    application="scripts/validations/silver_validation.py",
    conf={
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    },
    packages="io.delta:delta-spark_2.12:3.3.0",
    application_args=[
        "--silver_table_path", silver_table_path
    ],
    dag=dag
)

end = PythonOperator(
    task_id='end',
    python_callable=lambda: print("Jobs Completed successfully"),
    dag=dag,
)

start >> bronze_step >> silver_step >> silver_step_validation >> gold_step >> end
