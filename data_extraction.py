import pandas as pd
import tabula
import yaml
import requests
import time
import boto3
import json
from typing import List
from database_utils import DatabaseConnector


class DataExtractor:
    """Class to handle data extraction from various sources."""

    def __init__(self, connector: DatabaseConnector):
        self._connector = connector
        self._api_config = self.load_api_config()

    def read_rds_table(self, table_name: str) -> pd.DataFrame:
        """Get a DataFrame representation of a table from the database.

        Args:
            table_name (str): Name of the table to fetch.

        Raises:
            ValueError: If the table does not exist.

        Returns:
            pd.DataFrame: DataFrame representation of the database table.
        """
        tables = self._connector.list_db_tables()
        if table_name not in tables:
            raise ValueError(f"{table_name} table is not in the database.")

        return pd.read_sql_table(table_name, self._connector.engine)

    def retrieve_pdf_data(self, url: str) -> pd.DataFrame:
        """Return a DataFrame from tables in a PDF file.

        Args:
            url (str): The URL to the PDF file.

        Returns:
            pd.DataFrame: DataFrame representing the tables data in the PDF file.
        """
        dataframes: List[pd.DataFrame] = tabula.read_pdf(url, stream=True, pages='all')
        merged_dfs = pd.concat(dataframes, ignore_index=True)
        merged_dfs.reset_index(inplace=True)
        return merged_dfs

    def load_api_config(self) -> dict:
        """Load API configuration from file api_creds.yaml

        Returns:
            dict: Dictionary of configuration values.
        """
        with open("api_creds.yaml") as config:
            config = yaml.safe_load(config)
        return config

    def list_number_of_stores(self) -> int:
        """Return the number of stores in the API

        Returns:
            int: Store count
        """
        headers = self._api_config["header"]
        url = self._api_config["retrieve_store_count_url"]

        # Get the number of stores from the API
        response = requests.get(url, headers=headers)
        data = response.json()
        return int(data["number_stores"])

    def retrieve_stores_data(self) -> pd.DataFrame:
        """Retrieve a DataFrame that represents all the store data from the API.

        Returns:
            pd.DataFrame: DataFrame representing all store data.
        """
        number_of_stores = self.list_number_of_stores()
        headers = self._api_config["header"]
        url = self._api_config["retrieve_store_url"]

        store_jsons = []
        for index in range(number_of_stores):
            store_url = url + str(index)
            response = requests.get(store_url, headers=headers)
            store_jsons.append(response.json())
            time.sleep(self._api_config["request_delay"]) # sleep to avoid rate limit

        return pd.DataFrame(store_jsons)

    def extract_from_s3(self, s3_url: str, data_type: str = "csv") -> pd.DataFrame:
        """Download a CSV file from an S3 Bucket and parse it as a DataFrame

        Args:
            s3_url (str): Full URL of the file object.
            data_type (str, optional): Type of data to parse. Defaults to 'csv'. Valid is 'csv', 'json'

        Returns:
            pd.DataFrame: DataFrame representing the CSV file.
        """
        # Get bucket and file key from s3 url
        bucket, object_key = s3_url[5:].split('/', maxsplit=1)
        filename = object_key.split('/')[-1] # if nested, this gets file from end
        s3_client = boto3.client('s3')
        s3_client.download_file(bucket, object_key, filename)

        with open(filename, "r") as data_file:
            if data_type == "csv":
                data = pd.read_csv(data_file)
            elif data_type == "json":
                data = pd.read_json(data_file)

        return data

if __name__ == "__main__":
    connector = DatabaseConnector()
    instance = DataExtractor(connector)
    df = instance.read_rds_table("legacy_users")
    print(df.head(5))
