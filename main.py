from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

#Create connector for AWS database and our local database
rds_connector = DatabaseConnector(credential_path="db_creds.yaml")
local_connector = DatabaseConnector(credential_path="local_db_creds.yaml")

extractor = DataExtractor(rds_connector)
cleaner = DataCleaning()

# Clean up legacy_users and upload to our local database as dim_users
user_df = extractor.read_rds_table("legacy_users")
cleaned_user_df = cleaner.clean_user_data(user_df)
local_connector.upload_to_db(cleaned_user_df, 'dim_users')
