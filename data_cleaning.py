from datetime import datetime
from dateutil.parser import parse
from dateutil.parser._parser import ParserError
import phonenumbers
import re
import pandas as pd

class DataCleaning:
    """Class for cleaning data in a DataFrame"""

    def __init__(self):
        self.date_format = "%Y-%m-%d"
        self.date_format_alt = "%Y %B %d"

        # Regex patterns from https://regexr.com Community Patterns
        self.uuid_regex = r'^[0-9A-Za-z]{8}-[0-9A-Za-z]{4}-4[0-9A-Za-z]{3}-[89ABab][0-9A-Za-z]{3}-[0-9A-Za-z]{12}$'
        self.email_regex = r"^.+@.+\..+$"
        self.currency_regex = r"(\d*\.\d+|\d+)"
        self.expiry_date_format = "%m/%y"
        self.payment_date_format = "%Y-%m-%d"
        self.store_date_format = "%Y-%m-%d"
        self.product_date_format = "%Y-%m-%d"

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

        if phone.startswith("00"):
            phone = "+" + phone[2:]

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

    @staticmethod
    def try_parse_date(date: str) -> datetime | None:
        """Try to parse a date string using dateutils.parser

        Args:
            date (str): Date string to try to parse

        Returns:
            datetime | None: Parsed datetime, otherwise None on failure
        """
        if type(date) is not str:
            return None

        try:
            return parse(date)
        except ParserError:
            return None

    def parse_multiple_date_formats(self, dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
        """Parse a date column in a DataFrame that has multiple formats

        Args:
            dataframe (pd.DataFrame): DataFrame to apply parsing on
            column (str): Name of column for parsing

        Returns:
            pd.DataFrame: Parsed DataFrame
        """
        # First pass
        dataframe[column] = dataframe[column].apply(DataCleaning.try_parse_date)

        # Second pass
        parsed_dates = pd.to_datetime(
            dataframe[column], errors="coerce", format=self.date_format
        )
        # Third pass to capture other format
        mask = parsed_dates.isna()
        dataframe.loc[mask, column] = pd.to_datetime(
            dataframe.loc[mask, column] , errors="coerce", format=self.date_format_alt
        )
        # Combine results
        dataframe[column] = dataframe[column].combine_first(parsed_dates)
        return dataframe

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
        # Some of these dates are in different formats, need to perform two passes
        cleaned_df = self.parse_multiple_date_formats(cleaned_df, "date_of_birth")
        cleaned_df = self.parse_multiple_date_formats(cleaned_df, "join_date")

        # Null invalid UUID values
        # Pandas suggest using pd.NA over numpy.nan for string type columns.
        cleaned_df.loc[~cleaned_df.user_uuid.str.match(self.uuid_regex, na=False), "user_uuid"] = pd.NA

        # Some values are NULL which is not a pandas NaN. Replace those
        cleaned_df = cleaned_df.replace("NULL", pd.NA)

        # Some country codes are wrong
        cleaned_df.country_code = cleaned_df.country_code.replace("GGB", "GB")

        # Disabled phone number parsing for now... too many edge cases.

        # Using country code to parse phone number column to international format
        # phonenumbers library helps here but still not perfectly...
        # apply parse_phone_number on each row so we have access to country_code column
        #cleaned_df.phone_number = cleaned_df.apply(
        #    lambda row: self.parse_phone_number(row["phone_number"], row["country_code"]), axis=1 # type: ignore
        #)
        #cleaned_df.phone_number = cleaned_df.phone_number.astype("string") # Change column back to string from object


        # Ensure email addresses are valid
        # Some contain double @, remove duplicates
        cleaned_df.email_address = cleaned_df.email_address.str.replace(r'@+', '@', regex=True)
        # Match basic email regex
        cleaned_df.loc[~cleaned_df.email_address.str.match(self.email_regex, na=False), "email_address"] = pd.NA

        # Some rows contain NULL which is not a valid pandas NA value, so replace them
        cleaned_df = cleaned_df.replace("NULL", pd.NA)

        # Finally drop rows that have any null values
        cleaned_df = cleaned_df.dropna(how='any', axis='index')

        return cleaned_df

    @staticmethod
    def extract_from_combined_card_data(row: pd.Series) -> pd.Series:
        """Get card number and expiry date from the combined column

        Args:
            row (pd.Series): Row from card DataFrame

        Returns:
            pd.Series: Extracted card number and expiry date
        """
        if pd.isna(row["card_number"]) and pd.isna(row["expiry_date"]):
            split_parts = row["card_number expiry_date"].split(' ', 1)
            return pd.Series([split_parts[0], split_parts[1]])
        else:
            return pd.Series([row["card_number"], row["expiry_date"]])


    def clean_card_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Clean data for the card DataFrame.

        Args:
            dataframe (pd.DataFrame): The DataFrame that represents card data

        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        # Get data from combined column and split
        dataframe[["card_number", "expiry_date"]] = dataframe.apply(DataCleaning.extract_from_combined_card_data, axis=1)

        # Drop columns that are not required
        cleaned_df = dataframe.drop(columns=['card_number expiry_date', 'Unnamed: 0'])
        cleaned_df.card_number = cleaned_df.card_number.astype("string")

        # replace NULL with panda NA
        cleaned_df = cleaned_df.replace("NULL", pd.NA)

        # Remove any non-numeric characters from card_number
        cleaned_df.card_number = cleaned_df.card_number.replace(r'[^0-9]+', '', regex=True)
        cleaned_df.card_number = cleaned_df.card_number.replace('', pd.NA, regex=True)

        # Convert column to their respective types
        #cleaned_df.card_number = pd.to_numeric(cleaned_df.card_number, errors='coerce').astype("Int64") # This Dtype allows null values
        cleaned_df.card_provider = cleaned_df.card_provider.astype("string")
        cleaned_df.expiry_date = pd.to_datetime(
    cleaned_df.expiry_date, errors="coerce", format=self.expiry_date_format
        )

        # Parse payment confirmed date
        cleaned_df = self.parse_multiple_date_formats(cleaned_df, "date_payment_confirmed")

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
        cleaned_store_df = self.parse_multiple_date_formats(cleaned_store_df, "opening_date")

        # Only get rows whose country_code is a valid one
        country_codes = ["GB", "DE", "US"]
        mask = cleaned_store_df.country_code.isin(country_codes)
        cleaned_store_df = cleaned_store_df[mask]

        # Correct some mistyped continent names
        cleaned_store_df.loc[:, "continent"] = cleaned_store_df["continent"].str.replace("ee", "")

        # Web Portal has NA values but we want to keep it
        # Drop other rows that have NA values
        web_mask = cleaned_store_df["store_type"] == "Web Portal"
        cleaned_store_df = cleaned_store_df[web_mask | cleaned_store_df.drop(columns=["store_type"]).notna().all(axis=1)]

        return cleaned_store_df

    def convert_product_weights(self, weight: str) -> float | None:
        """Convert a string weight value to kilograms.

        - Strings ending in kg return just the value
        - Strings ending in g will either:
            - return just the value if there is a decimal, assumption that it's kilograms
            - return the value divided by 1000 to convert to kilograms
        - Strings ending in ml will be assumed 1:1 to grams and therefore divided by 1000 for kilograms
        - Strings with an 'x' are assumed to be a multiplier i.e '12 x 125g' and therefore the final output
          will be 0.125 * 12 = 1.25 (kg)

        Args:
            weight (str): String representing a weight to be parsed.

        Returns:
            float | None: The weight, in kilograms.
        """
        # value is N/A
        if type(weight) is not str:
            return

        # Some weights have multipliers, like 12 x 250g
        if "x" in weight:
            multiplier, weight = weight.split("x")
            multiplier, weight = int(multiplier.strip()), weight.strip()
        else:
            multiplier = 1

        # pattern creates two groups, one of which is the float value of the weight, the other being the unit
        # ([\d.]+) digit or a dot
        # ([a-zA-Z]+) letters
        pattern_matches = re.match(r'([\d.]+)([a-zA-Z]+)', weight)

        # No matches so likely not a weight
        if not pattern_matches:
            return

        # Try to convert value and unit
        try:
            value = float(pattern_matches.group(1))
            unit = pattern_matches.group(2).lower()
        except ValueError:
            # value is not a float, so must be invalid
            return

        multiplied_value = multiplier * value

        if unit == "g":
            # Some gram values are meant to be kg, denoted by decimal
            # i.e 1.2g should be 1.2kg
            if not value.is_integer():
                return multiplied_value
            else:
                return multiplied_value / 1000
        elif unit == "ml":
            return multiplied_value / 1000
        elif unit == "kg":
            # Already kilos
            return multiplied_value
        elif unit == "oz":
            # Convert oz to kg
            return multiplied_value / 35.274
        # Unknown unit or other
        return

    def clean_products_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Clean data for the Products DataFrame

        Args:
            dataframe (pd.DataFrame): DataFrame representing products data

        Returns:
            pd.DataFrame: Cleaned Products DataFrame
        """
        # Drop additional index column
        cleaned_csv_data = dataframe.drop(columns=["Unnamed: 0"])
        product_code_test = "A8-4686892S"
        print("------------ BEFORE ------------")
        print(cleaned_csv_data[cleaned_csv_data.product_code == product_code_test])

        # Convert weights to floats
        cleaned_csv_data.weight = cleaned_csv_data.weight.apply(self.convert_product_weights)
        cleaned_csv_data.weight = pd.to_numeric(cleaned_csv_data.weight, errors="coerce").astype("float")

        # Only allow removed or still_available, null other options
        valid_removed_values = ["Removed", "Still_avaliable"]
        cleaned_csv_data.loc[~cleaned_csv_data.removed.isin(valid_removed_values)] = pd.NA
        # Convert to boolean since less storage
        cleaned_csv_data.removed = cleaned_csv_data.removed.map({"Removed": True, "Still_avaliable": False}).astype("boolean")

        # Check UUID format is correct
        cleaned_csv_data.loc[~cleaned_csv_data.uuid.str.match(self.uuid_regex, na=False), "uuid"] = pd.NA

        # Convert product_price to float, remove any characters before the number
        cleaned_csv_data.product_price = cleaned_csv_data.product_price.str.extract(self.currency_regex)
        cleaned_csv_data.product_price = cleaned_csv_data.product_price.astype("Float64")

        # Convert column types
        cleaned_csv_data = cleaned_csv_data.astype(
            {
                "product_name": "string",
                "category": "string",
                "product_code": "string",
                "uuid": "string"
            }
        )
        cleaned_csv_data.EAN = pd.to_numeric(cleaned_csv_data.EAN, errors="coerce").astype("int64", errors="ignore")

        # Convert date column
        cleaned_csv_data = self.parse_multiple_date_formats(cleaned_csv_data, "date_added")

        print("------------ AFTER ------------")
        print(cleaned_csv_data[cleaned_csv_data.product_code == product_code_test])

        # Finally drop any rows with NA values
        cleaned_csv_data = cleaned_csv_data.dropna(how="any", axis="index")

        return cleaned_csv_data

    def clean_orders_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Clean data for the Orders DataFrame

        Args:
            dataframe (pd.DataFrame): DataFrame representing Order data

        Returns:
            pd.DataFrame: Cleaned Orders DataFrame
        """
        # Drop unnecessary columns
        cleaned_orders_df = dataframe.drop(columns=["level_0", "index", "first_name", "last_name", "1"])
        # Ensure columns are correct type
        cleaned_orders_df = cleaned_orders_df.astype(
            {
                "date_uuid": "string",
                "user_uuid": "string",
                "store_code": "string",
                "product_code": "string"
            }
        )
        return cleaned_orders_df

    def clean_date_details_data(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Clean data for the Date Details DataFrame

        Args:
            dataframe (pd.DataFrame): DataFrame representing Date Details data

        Returns:
            pd.DataFrame: Cleaned Date Details DataFrame
        """
        valid_time_periods = [
            "Morning",
            "Midday",
            "Evening",
            "Late_Hours"
        ]
        # Ensure time_period values are those that are valid
        dataframe.loc[~dataframe.time_period.isin(valid_time_periods)] = pd.NA

        # Combine date data into one column
        dataframe["datetime_str"] = (
            dataframe.day.astype("str")
            + "-"
            + dataframe.month.astype("str")
            + "-"
            + dataframe.year.astype("str")
            + " "
            + dataframe.timestamp
        )
        # Convert combined column into datetime
        dataframe["datetime"] = pd.to_datetime(dataframe["datetime_str"], format="%d-%m-%Y %H:%M:%S", errors="coerce")
        dataframe = dataframe.drop(columns=["datetime_str", "day", "month", "year", "timestamp"])

        # Check UUID format
        dataframe.loc[~dataframe.date_uuid.str.match(self.uuid_regex, na=False), "date_uuid"] = pd.NA

        # Convert types
        dataframe = dataframe.astype(
            {
                "time_period": "string",
                "date_uuid": "string"
            }
        )

        # Drop any rows with null values
        dataframe = dataframe.dropna(how="any", axis="index")
        return dataframe


if __name__ == "__main__":
    from database_utils import DatabaseConnector
    from data_extraction import DataExtractor

    connector = DatabaseConnector()
    extractor = DataExtractor(connector)
    df = extractor.read_rds_table("legacy_users")
    cleaner = DataCleaning()

    cleaned_df = cleaner.clean_user_data(df)
    print(cleaned_df.info())
