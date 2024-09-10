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
        self.expiry_date_format = "%m/%y"
        self.payment_date_format = "%Y-%m-%d"
        self.store_date_format= "%Y-%m-%d"

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

    def clean_card_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Clean data for the card DataFrame.

        Args:
            dataframe (pd.DataFrame): The DataFrame that represents card data

        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        # Drop columns that have been incorrectly detected by tabula
        cleaned_df = dataframe.drop(columns=['card_number expiry_date', 'Unnamed: 0'])


        # replace NULL with panda NA
        cleaned_df = cleaned_df.replace("NULL", pd.NA)

        # Remove any non-numeric characters from card_number
        cleaned_df.card_number = cleaned_df.card_number.replace(r'[^0-9]+', '', regex=True)
        cleaned_df.card_number = cleaned_df.card_number.replace('', pd.NA, regex=True)

        # Convert column to their respective types
        cleaned_df.card_number = pd.to_numeric(cleaned_df.card_number, errors='coerce').astype("Int64") # This Dtype allows null values
        cleaned_df.card_provider = cleaned_df.card_provider.astype("string")
        cleaned_df.expiry_date = pd.to_datetime(
    cleaned_df.expiry_date, errors="coerce", format=self.expiry_date_format
        )
        cleaned_df.date_payment_confirmed = pd.to_datetime(
            cleaned_df.date_payment_confirmed, errors="coerce", format=self.payment_date_format
        )

        # Finally drop any rows that have null values
        cleaned_df = cleaned_df.dropna(how='any', axis='index')
        return cleaned_df

    def clean_store_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Clean data for the store DataFrame

        Args:
            dataframe (pd.DataFrame): The DataFrame that represents store data

        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        # Replace N/A string with pandas NA
        cleaned_store_df = dataframe.replace('N/A', pd.NA)

        # Replace pythonic None from json response to pandas NA
        cleaned_store_df = cleaned_store_df.replace([None], pd.NA)

        # Noticed the lat column appears useless, since latitude exists with values. So drop it
        cleaned_store_df = cleaned_store_df.drop(columns=['lat'])

        # Change column types
        cleaned_store_df = cleaned_store_df.astype(
            {
                "address": "string",
                "locality": "string",
                "store_code": "string",
                "store_type": "string",
                "country_code": "string",
                "continent": "string"
            }
        )

        # Remove letters from any numbers in staff numbers
        cleaned_store_df.staff_numbers = cleaned_store_df.staff_numbers.replace(r'[^0-9]+', '', regex=True)
        cleaned_store_df.staff_numbers = cleaned_store_df.staff_numbers.replace('', pd.NA, regex=True)
        cleaned_store_df.staff_numbers = pd.to_numeric(cleaned_store_df.staff_numbers, errors="coerce").astype("Int64")

        # Convert long/latitude to float
        cleaned_store_df.longitude = pd.to_numeric(cleaned_store_df.longitude, errors="coerce").astype("float")
        cleaned_store_df.latitude = pd.to_numeric(cleaned_store_df.latitude, errors="coerce").astype("float")

        # Convert opening date
        cleaned_store_df.opening_date = pd.to_datetime(cleaned_store_df.opening_date, errors="coerce", format=self.store_date_format)

        # Only get rows whose country_code is a valid one
        country_codes = ["GB", "DE", "US"]
        mask = cleaned_store_df.country_code.isin(country_codes)
        cleaned_store_df = cleaned_store_df[mask]

        # Correct some mistyped continent names
        cleaned_store_df.loc[:, "continent"] = cleaned_store_df["continent"].str.replace("ee", "")

        # Finally drop any rows with null values
        cleaned_store_df = cleaned_store_df.dropna(how="any", axis="index")

        return cleaned_store_df

if __name__ == "__main__":
    from database_utils import DatabaseConnector
    from data_extraction import DataExtractor

    connector = DatabaseConnector()
    extractor = DataExtractor(connector)
    df = extractor.read_rds_table("legacy_users")
    cleaner = DataCleaning()

    cleaned_df = cleaner.clean_user_data(df)
    print(cleaned_df.info())
