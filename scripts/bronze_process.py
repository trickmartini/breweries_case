import argparse
import logging

import requests
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, when, col
from delta import *
from pyspark.sql.types import TimestampType


def get_api_data(retries: int, per_page_values: int, url: str, params: None, logger: logging.Logger):
    """
   This function retrieves data from the given API URL, handling pagination and retries
    in case of temporary failures. It returns all fetched records as a list.

    @param retries: The maximum number of retry attempts in case of request failure.
    @param per_page_values: Number of records to fetch per page.
    @param url: The API endpoint URL.
    @param params: Optional query parameters for the API request (default: None).
    @param logger: Logger instance for logging events and errors.
    @return: A list containing all retrieved API records.
    @raises Exception: If the maximum number of retries is exceeded.
    """
    session = requests.session()
    all_data = []
    page = 1

    while True:
        for attempt in range(retries):
            try:
                if params is None:
                    params = {}
                params['page'] = page
                params['per_page'] = per_page_values
                response = session.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                # if page is empty, return
                if not data:
                    return all_data

                all_data.extend(data)
                break
            except requests.exceptions.RequestException as e:
                logger.info("Exception: {}".format(e))
        else:
            raise Exception("Too many retries")
        page += 1
    return all_data


def raw_data_ingestion(logger: logging.Logger, raw_path: str, url: str):
    """
    Extracts raw data from an API and saves it as a JSON file in the specified directory.

    This function fetches data from the given API URL using `get_api_data` and stores it
    in a JSON file within the specified raw data directory. If the directory does not exist,
    it is created before saving the file.

    @param logger: Logger instance for logging events and errors.
    @param raw_path: Directory path where the raw JSON file will be stored.
    @param url: API endpoint URL from which the data will be retrieved.

    @raises ValueError: If an error occurs while writing the JSON file.

    @return: None
    """
    if not os.path.isdir(raw_path):
        os.makedirs(raw_path)

    output_file = os.path.join(raw_path, "brew.json")
    logger.info("Downloading data from {}".format(url))

    raw_data = get_api_data(retries=3, per_page_values=50, url=url, params=None, logger=logger)

    try:
        with open(output_file, "w") as f:
            json.dump(raw_data, f)
        logger.info(f"Data saved in {output_file}")
    except Exception as e:
        logger.warning(f"Error trying write file: {e}")
        raise ValueError(f"Error trying write file: {e}")


def write_api_data(spark: SparkSession, raw_path: str, logger: logging.Logger, output_path_current: str):
    """
    Processes raw API data and writes it to a Delta Lake table, implementing Slowly Changing Dimensions (SCD) Type 2.

    @param spark: SparkSession instance for handling data processing.
    @param raw_path: Path to the raw JSON file containing API data.
    @param logger: Logger instance for logging events and errors.
    @param output_path_current: Path to the Delta Lake table where processed data will be stored.

    @raises ValueError: If an error occurs while reading or writing data.

    @return: None
    """
    logger.info(f"Reading {raw_path}")

    api_data = spark.read.json(raw_path)

    if DeltaTable.isDeltaTable(spark, output_path_current):
        # close record (end_timestamp -> current_timestamp)
        current_data = DeltaTable.forPath(spark, output_path_current)
        current_data.alias("c") \
            .merge(
            api_data.alias("n"),
            "c.id = n.id and c.end_timestamp is null") \
            .whenMatchedUpdate(
            condition="""
                    coalesce(c.address_1, '') <> coalesce(n.address_1, '') OR
                    coalesce(c.address_2, '') <> coalesce(n.address_2, '') OR
                    coalesce(c.address_3, '') <> coalesce(n.address_3, '') OR
                    coalesce(c.brewery_type, '') <> coalesce(n.brewery_type, '') OR
                    coalesce(c.city, '') <> coalesce(n.city, '') OR
                    coalesce(c.country, '') <> coalesce(n.country, '') OR
                    coalesce(c.latitude, 0) <> coalesce(n.latitude, 0) OR
                    coalesce(c.longitude, 0) <> coalesce(n.longitude, 0) OR
                    coalesce(c.name, '') <> coalesce(n.name, '') OR
                    coalesce(c.phone, '') <> coalesce(n.phone, '') OR
                    coalesce(c.postal_code, '') <> coalesce(n.postal_code, '') OR
                    coalesce(c.state, '') <> coalesce(n.state, '') OR
                    coalesce(c.state_province, '') <> coalesce(n.state_province, '') OR
                    coalesce(c.street, '') <> coalesce(n.street, '') OR
                    coalesce(c.website_url, '') <> coalesce(n.website_url, '')
                                    """,

            set={
                "end_timestamp": current_timestamp()
            }
        ) \
            .execute()
        # insert new record
        current_data.alias("c") \
            .merge(
            api_data.alias("n"),
            "c.id = n.id and c.end_timestamp is null") \
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
                "street": "n.street",
                "website_url": "n.website_url",
                "start_timestamp": current_timestamp(),
                "end_timestamp": lit(None)
            }
        ) \
            .execute()
    else:
        api_data = (api_data
                    .withColumn("start_timestamp", current_timestamp())
                    .withColumn("end_timestamp", lit(None).cast(TimestampType()))
                    )
        api_data.write.format("delta").mode("overwrite").partitionBy("country").save(output_path_current)


if __name__ == "__main__":
    # setup
    builder = SparkSession.builder \
        .appName("bronze layer") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    logger = spark._jvm.org.apache.log4j.LogManager.getLogger("breweries_transformation")

    parser = argparse.ArgumentParser(description="Bronze layer Args")
    parser.add_argument("--bronze_raw_files", required=True, help="Json Bronze files")
    parser.add_argument("--bronze_table_path", required=True, help="Bronze table path")
    parser.add_argument("--api_url", required=True, help="API URL")

    args = parser.parse_args()

    raw_path = args.bronze_raw_files
    output_path = args.bronze_table_path
    url = args.api_url

    raw_data_ingestion(logger, args.bronze_raw_files, url)
    write_api_data(spark, raw_path, logger, output_path)
