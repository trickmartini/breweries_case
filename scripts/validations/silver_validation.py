import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# silver validation
# This process verifies the integrity of the Silver layer table, by checking for inconsistent data.
# If any records contain null values in critical fields, an error is raised to prevent downstream issues.
def silver_validation(spark: SparkSession, silver_table_path: str, logger: logging.Logger):

    #read data from raw layer
    logger.info(f"Reading {silver_table_path}")
    silver_data = spark.read.format("delta").load(silver_table_path)
    invalid_data = silver_data.filter(
        (col("country").isNull()) |
        (col("id").isNull()) |
        (col("state").isNull()) |
        (col("city").isNull()) |
        (col("brewery_type").isNull())
    )
    invalid_records_count = invalid_data.count()

    logger.info(f"Total invalid records found: {invalid_records_count}")

    if invalid_records_count > 0:
        raise ValueError(f"Found {invalid_records_count} rows with inconsistent data in {silver_table_path}")

if __name__ == "__main__":
    # setup
    spark = SparkSession.builder.appName("silver_validation").getOrCreate()
    logger = spark._jvm.org.apache.log4j.LogManager.getLogger("silver_validation")
    parser = argparse.ArgumentParser(description="Bronze layer Args")

    parser.add_argument("--silver_table_path", required=True, help="Bronze table path")
    args = parser.parse_args()
    output_path = args.silver_table_path

    silver_validation(spark, output_path, logger)