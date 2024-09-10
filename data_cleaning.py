import phonenumbers
import re
import pandas as pd


class DataCleaning:
    """Class for cleaning data in a DataFrame"""
    
    def __init__(self):
        self.date_format = "%Y-%m-%d"

        # Regex patterns from https://regexr.com Community Patterns
        self.uuid_regex = r'^[0-9A-Za-z]{8}-[0-9A-Za-z]{4}-4[0-9A-Za-z]{3}-[89ABab][0-9A-Za-z]{3}-[0-9A-Za-z]{12}$'
        self.email_regex = r"^[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$"

    def parse_phone_number(self, phone: str, region: str) -> str | None:
        """Parse a phone number from a string, given the region for the number.

        Args:
            phone (str): The phone number string to parse.
            region (str): The region of the phone number owner.

        Returns:
            str | None: Parsed phone number, otherwise None if not parsed.
        """
        # Ignore already NA values
        if type(phone) is not str:
            return None

        # Clean the phone number by removing (0), extensions, and other unnecessary characters
        phone = re.sub(r'\(0\)', '', phone)  # Remove (0)
        phone = phone.replace("(", "").replace(")", "")  # Remove parentheses
        phone = re.sub(r'x.*$', '', phone)  # Remove extensions (for example x1234)
        phone = re.sub(r'[^\d+]', '', phone)  # Remove non-numeric characters except for +

        try:
            # Attempt to parse the number with the phonenumbers library
            # If no '+' sign, assume it's a local number and use the passed region
            if not phone.startswith('+'):
                parsed_number = phonenumbers.parse(phone, region)
            else:
                parsed_number = phonenumbers.parse(phone)

            # Format the parsed number in international format
            return phonenumbers.format_number(parsed_number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)

        except phonenumbers.phonenumberutil.NumberParseException:
            # Number could not be parsed
            return None

    def clean_user_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Clean data for the user DataFrame from the database.

        Args:
            dataframe (pd.DataFrame): The DataFrame that represents the user data

        Returns:
            DataFrame: Cleaned DataFrame
        """
        # Convert object columns to their respective type
        cleaned_df = dataframe.astype(
            {
                "first_name": "string",
                "last_name": "string",
                "company": "string",
                "email_address": "string",
                "address": "string",
                "country_code": "string",
                "country": "string",
                "user_uuid": "string",
            }
        )

        # Convert object date columns to the datetime type
        cleaned_df.date_of_birth = pd.to_datetime(
            cleaned_df.date_of_birth, errors="coerce", format=self.date_format
        )
        cleaned_df.join_date = pd.to_datetime(
            cleaned_df.join_date, errors="coerce", format=self.date_format
        )

        # Null invalid UUID values
        # Pandas suggest using pd.NA over numpy.nan for string type columns.
        cleaned_df.loc[~cleaned_df.user_uuid.str.match(self.uuid_regex, na=False), "user_uuid"] = pd.NA

        # Some values are NULL which is not a pandas NaN. Replace those
        cleaned_df = cleaned_df.replace("NULL", pd.NA)

        # Some country codes are wrong
        cleaned_df.country_code = cleaned_df.country_code.replace("GGB", "GB")

        # Using country code to parse phone number column to international format
        # phonenumbers library helps here but still not perfectly...
        # apply parse_phone_number on each row so we have access to country_code column
        cleaned_df.phone_number = cleaned_df.apply(
            lambda row: self.parse_phone_number(row["phone_number"], row["country_code"]), axis=1 # type: ignore
        )
        cleaned_df.phone_number = cleaned_df.phone_number.astype("string") # Change column back to string from object

        # Ensure email addresses are valid
        cleaned_df.loc[~cleaned_df.email_address.str.match(self.email_regex, na=False), "email_address"] = pd.NA

        # Some rows contain NULL which is not a valid pandas NA value, so replace them
        cleaned_df = cleaned_df.replace("NULL", pd.NA)

        # Finally drop rows that have any null values
        cleaned_df = cleaned_df.dropna(how='any', axis='index')

        return cleaned_df

if __name__ == "__main__":
    from database_utils import DatabaseConnector
    from data_extraction import DataExtractor

    connector = DatabaseConnector()
    extractor = DataExtractor(connector)
    df = extractor.read_rds_table("legacy_users")
    cleaner = DataCleaning()

    cleaned_df = cleaner.clean_user_data(df)
    print(cleaned_df.info())
