#%%
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

#%%
#Create connector for AWS database and our local database
rds_connector = DatabaseConnector(credential_path="db_creds.yaml")
local_connector = DatabaseConnector(credential_path="local_db_creds.yaml")

extractor = DataExtractor(rds_connector)
cleaner = DataCleaning()

#%%
# Clean up legacy_users and upload to our local database as dim_users
user_df = extractor.read_rds_table("legacy_users")
cleaned_user_df = cleaner.clean_user_data(user_df)
local_connector.upload_to_db(cleaned_user_df, "dim_users")

#%%
# Get PDF url from file
with open("pdf_url.txt", "r") as url_file:
    url = url_file.readline().strip()

# Clean up card details PDF document and upload to our local database as dim_card_details
card_details = extractor.retrieve_pdf_data(url)
cleaned_card_details = cleaner.clean_card_data(card_details)
local_connector.upload_to_db(cleaned_card_details, "dim_card_details")

# %%
# Clean up store data and upload to our local database as dim_store_details
store_details = extractor.retrieve_stores_data()
cleaned_store_details = cleaner.clean_store_data(store_details)
local_connector.upload_to_db(cleaned_store_details, "dim_store_details")

# %%
# Get S3 Bucket URL for CSV file
with open("bucket_url.txt", "r") as url_file:
    url = url_file.readline().strip()

# %%
# Clean up product data and upload to our local database as dim_products
product_details = extractor.extract_from_s3(url)
cleaned_product_details = cleaner.clean_products_data(product_details)
local_connector.upload_to_db(cleaned_product_details, "dim_products")
# %%
