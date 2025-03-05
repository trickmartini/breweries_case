import argparse

from pyspark.sql import SparkSession
from delta import *
import logging


def gold_layer_process(spark: SparkSession, logger: logging.Logger, input_path: str, output_path: str):
    """
      Aggregates brewery data from the Silver Layer to create a summarized dataset in the Gold Layer.

      @param spark: SparkSession instance for handling data processing.
      @param logger: Logger instance for logging events and errors.
      @param input_path: Path to the Silver Layer Delta table.
      @param output_path: Path to store the aggregated Gold Layer Delta table.

      @raises ValueError: If the input Silver Layer table is empty.

      @return: None
      """

    logger.info(f"Reading data from: {input_path}")
    # Read data from silver layer
    silver_data = spark.read.format("delta").load(input_path)

    if silver_data.rdd.isEmpty():
        logger.error("Input data is empty. No process will be performed.")
        raise ValueError("Input data is empty.")

    detailed_location = (
        silver_data
        .groupBy("country", "state", "city", "brewery_type")
        .count().alias("qty_breweries")
    )

    if detailed_location.rdd.isEmpty():
        logger.error("Output dataframe is empty. No process will be performed.")
        raise ValueError("Output dataframe is empty. No data to write.")

    detailed_location.write.format("delta").mode("overwrite").partitionBy("country").save(output_path)

    logger.info(f"Data written to: {output_path}")


if __name__ == "__main__":
    builder = SparkSession.builder \
        .appName("gold_layer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    logger = spark._jvm.org.apache.log4j.LogManager.getLogger("breweries_transformation")

    parser = argparse.ArgumentParser(description="Gold layer Args")

    parser.add_argument("--silver_table_path", required=True, help="Bronze table path")
    parser.add_argument("--gold_table_path", required=True, help="Gold table path")

    args = parser.parse_args()

    input_path = args.silver_table_path
    output_path = args.gold_table_path

    gold_layer_process(spark, logger, input_path, output_path)
