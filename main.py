import time
from functools import wraps
from typing import Callable

from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def notify_time(func_name: str):
    """Outputs when the function starts, and the duration it took for it to complete

    Args:
        func_name (str): Friendly name of the function.
    """
    def decorator(function: Callable):
        @wraps(function)
        def wrapper(*args, **kwargs):
            print(f"{func_name}: started.")
            start_time = time.time()
            return_val = function(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            print(f"{func_name} finished in {execution_time} seconds.")
            return return_val
        return wrapper
    return decorator

class DataApplication:
    """Class for handling order of data processing. Use `run()` method to execute correct order."""

    def __init__(self, remote_credentials: str = "db_creds.yaml", local_credentials: str = "local_db_creds.yaml"):
        #Create connector for AWS database and our local database
        self.rds_connector = DatabaseConnector(credential_path=remote_credentials)
        self.local_connector = DatabaseConnector(credential_path=local_credentials)

        self.extractor = DataExtractor(self.rds_connector)
        self.cleaner = DataCleaning()

    def read_url_from_file(self, path: str) -> str:
        with open(path, "r") as url_file:
            url = url_file.readline().strip()
        return url

    @notify_time("User Details")
    def clean_legacy_users(self):
        """Run extract and clean methods for user details data."""
        # Clean up legacy_users and upload to our local database as dim_users
        user_df = self.extractor.read_rds_table("legacy_users")
        cleaned_user_df = self.cleaner.clean_user_data(user_df)
        self.local_connector.upload_to_db(cleaned_user_df, "dim_users")

    @notify_time("Card Details")
    def clean_card_details(self):
        """Run extract and clean methods for card details data."""
        # Get PDF url from file
        url = self.read_url_from_file("pdf_url.txt")

        # Clean up card details PDF document and upload to our local database as dim_card_details
        card_details = self.extractor.retrieve_pdf_data(url)
        cleaned_card_details = self.cleaner.clean_card_data(card_details)
        self.local_connector.upload_to_db(cleaned_card_details, "dim_card_details")

    @notify_time("Store Details")
    def clean_store_details(self):
        """Run extract and clean methods for store details data."""
        # Clean up store data and upload to our local database as dim_store_details
        store_details = self.extractor.retrieve_stores_data()
        cleaned_store_details = self.cleaner.clean_store_data(store_details)
        self.local_connector.upload_to_db(cleaned_store_details, "dim_store_details")


    @notify_time("Product Details")
    def clean_product_details(self):
        """Run extract and clean methods for product details data."""
        # Get S3 Bucket URL for CSV file
        url = self.read_url_from_file("product_bucket_url.txt")

        # Clean up product data and upload to our local database as dim_products
        product_details = self.extractor.extract_from_s3(url)
        cleaned_product_details = self.cleaner.clean_products_data(product_details)
        self.local_connector.upload_to_db(cleaned_product_details, "dim_products")

    @notify_time("Order Details")
    def clean_order_details(self):
        """Run extract and clean methods for order details data."""
        # Clean up order data and upload to our local database as orders_table
        orders_details = self.extractor.read_rds_table("orders_table")
        cleaned_order_details = self.cleaner.clean_orders_data(orders_details)
        self.local_connector.upload_to_db(cleaned_order_details, "orders_table")


    @notify_time("Date Details")
    def clean_date_details(self):
        """Run extract and clean methods for date details data."""
        # Get date details JSON URL from file
        url = self.read_url_from_file("date_bucket_url.txt")

        # Clean up date details and upload to our local database as dim_date_times
        date_details = self.extractor.extract_from_s3(url, data_type="json")
        cleaned_date_details = self.cleaner.clean_date_details_data(date_details)
        self.local_connector.upload_to_db(cleaned_date_details, "dim_date_times")

    @notify_time("Application")
    def run(self):
        """Run each extraction and clean methods"""
        self.clean_legacy_users()
        self.clean_card_details()
        self.clean_store_details()
        self.clean_product_details()
        self.clean_order_details()
        self.clean_date_details()

if __name__ == "__main__":
    app = DataApplication()
    app.run()
