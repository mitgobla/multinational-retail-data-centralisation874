import pandas as pd
import tabula
from typing import List
from database_utils import DatabaseConnector


class DataExtractor:
    """Class to handle data extraction from various sources."""

    def __init__(self, connector: DatabaseConnector):
        self._connector = connector

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

if __name__ == "__main__":
    connector = DatabaseConnector()
    instance = DataExtractor(connector)
    df = instance.read_rds_table("legacy_users")
    print(df.head(5))
