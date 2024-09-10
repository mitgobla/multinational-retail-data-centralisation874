import yaml
import sqlalchemy
import pandas as pd
from typing import List


class DatabaseConnector:
    """Class to handle database connection."""

    def __init__(
        self,
        credential_path: str = "db_creds.yaml",
        db_type: str = "postgresql",
        db_api: str = "psycopg2",
    ):
        self._engine: sqlalchemy.Engine | None = None
        self._credential_path = credential_path
        self._db_type = db_type
        self._db_api = db_api

    @property
    def engine(self) -> sqlalchemy.Engine:
        if self._engine is None:
            self._engine = self.init_db_engine()

        return self._engine

    def read_db_creds(self) -> dict:
        """Load and return the database credentials from file.

        Args:
            filepath (str, optional): Path to credentials file. Defaults to "db_creds.yaml".

        Returns:
            dict: Dictionary of credential file configuration.
        """

        with open(self._credential_path, "r") as f:
            creds = yaml.safe_load(f)
        return creds

    def init_db_engine(self) -> sqlalchemy.Engine:
        """Get an engine instance by connecting to a database instance

        Args:
            db_type (str, optional): Type of database engine. Defaults to "postgresql".
            db_api (str, optional): Type of Python API for database type. Defaults to "psycopg2".

        Returns:
            sqlalchemy.Engine: SQLAlchemy engine to perform operations.
        """

        creds = self.read_db_creds()
        return sqlalchemy.create_engine(
            url=f"{self._db_type}+{self._db_api}://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}",
        )

    def list_db_tables(self) -> List[str]:
        """Return a list of tables in the database.

        Returns:
            List[str]: List of tables, empty if none.
        """
        inspector: sqlalchemy.Inspector | None = sqlalchemy.inspect(self.engine)

        if inspector:
            return inspector.get_table_names()

    def upload_to_db(
        self, dataframe: pd.DataFrame, table_name: str, replace: bool = True
    ):
        """Upload a DataFrame as a table to the database.

        Args:
            dataframe (pd.DataFrame): The DataFrame to upload.
            table_name (str): The name of the table for the DataFrame data
            replace (bool, optional): Replace table if already exists. Defaults to True.
        """
        dataframe.to_sql(
            table_name, self.engine, if_exists="replace" if replace else "fail", index=False
        )


if __name__ == "__main__":
    instance = DatabaseConnector()
    print(instance.list_db_tables())
