import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, trim, concat, lit, regexp_replace, row_number
from pyspark.sql.types import DoubleType
from delta import *


def silver_layer_process(spark: SparkSession, logger: logging.Logger, bronze_table_path: str, output_path: str):
    """
    Transforms and standardizes raw brewery data from the Bronze Layer before storing it in the Silver Layer.

    @param spark: SparkSession instance for processing data.
    @param logger: Logger instance for tracking the execution process.
    @param bronze_table_path: Path to the Bronze Layer Delta table (raw data).
    @param output_path: Path to store the transformed Silver Layer Delta table.

    @raises ValueError: If the Bronze Layer table is empty, the process is aborted.

    @return: None
    """
    primary_key = "id"
    columns = ['address_1', 'address_2', 'address_3', 'brewery_type', 'city', 'latitude', 'longitude', 'name', 'phone',
               'postal_code', 'state', 'state_province', 'website_url', 'country']

    if not DeltaTable.isDeltaTable(spark, bronze_table_path):
        logger.warning("Raw data is empty. No process will be performed.")
        raise ValueError("Raw data is empty.")

    bronze_data = spark.read.format("delta").load(bronze_table_path)

    silver_data = (bronze_data
                   .filter(col("end_timestamp").isNull())
                   .withColumn("country", trim(col("country")))
                   .withColumn("address_1", trim(col("address_1")))
                   .withColumn("address_2", trim(col("address_2")))
                   .withColumn("address_3", trim(col("address_3")))
                   .withColumn("city", trim(col("city")))
                   .withColumn("state", trim(col("state")))
                   .withColumn("state_province", trim(col("state_province")))
                   .withColumn("brewery_type", trim(col("brewery_type")))
                   .withColumn("name", trim(col("name")))
                   .withColumn("phone", concat(lit("+"), regexp_replace(col("phone"), "[^0-9]",
                                                                        "")))  # normalized to E.164 format
                   # cast latitude and longitude as double to maximize compatibility with BI tools, for geographical KPI.
                   .withColumn("latitude", col("latitude").cast(DoubleType()))
                   .withColumn("longitude", col("longitude").cast(DoubleType()))
                   )

    if not DeltaTable.isDeltaTable(spark, output_path):
        write_df = silver_data.select([primary_key] + columns)\
                    .withColumn("update_timestamp", current_timestamp())

        write_df.write.format("delta").mode("overwrite").partitionBy("country").save(output_path)
        logger.info(f"Data saved in current silver layer: {output_path}")

    else:
        current_table = DeltaTable.forPath(spark, output_path)

        current_table.alias("c") \
            .merge(
            silver_data.alias("n"),
            "c.id = n.id") \
            .whenNotMatchedInsert(
            values={
                "address_1": "n.address_1",
                "address_2": "n.address_2",
                "address_3": "n.address_3",
                "brewery_type": "n.brewery_type",
                "city": "n.city",
                "country": "n.country",
                "id": "n.id",
                "latitude": "n.latitude",
                "longitude": "n.longitude",
                "name": "n.name",
                "phone": "n.phone",
                "postal_code": "n.postal_code",
                "state": "n.state",
                "state_province": "n.state_province",
                "website_url": "n.website_url",
                "update_timestamp": current_timestamp(),
            }
        ) \
            .whenMatchedUpdate(
            condition="""c.update_timestamp < n.start_timestamp""",
            set={
                "address_1": "n.address_1",
                "address_2": "n.address_2",
                "address_3": "n.address_3",
                "brewery_type": "n.brewery_type",
                "city": "n.city",
                "country": "n.country",
                "id": "n.id",
                "latitude": "n.latitude",
                "longitude": "n.longitude",
                "name": "n.name",
                "phone": "n.phone",
                "postal_code": "n.postal_code",
                "state": "n.state",
                "state_province": "n.state_province",
                "website_url": "n.website_url",
                "update_timestamp": current_timestamp()
            }
        ) \
            .execute()


if __name__ == "__main__":
    builder = SparkSession.builder \
        .appName("silver_layer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    logger = spark._jvm.org.apache.log4j.LogManager.getLogger("breweries_transformation")

    parser = argparse.ArgumentParser(description="Bronze layer Args")
    parser.add_argument("--bronze_table_path", required=True, help="Bronze table path")
    parser.add_argument("--silver_table_path", required=True, help="Silver table path")

    args = parser.parse_args()

    bronze_path = args.bronze_table_path
    output_path_current = args.silver_table_path

    silver_layer_process(spark, logger, bronze_path, output_path_current)
