# Multinational Retail Data Centralisation

The purpose of this repository is to do the following:

1. Extract and clean the current data
2. Create the database schema
3. Query the data

## Table of Contents

- [Multinational Retail Data Centralisation](#multinational-retail-data-centralisation)
  - [Table of Contents](#table-of-contents)
  - [Extracting and cleaning data](#extracting-and-cleaning-data)
    - [Sources](#sources)
    - [Methodology](#methodology)
  - [Installation instructions](#installation-instructions)
    - [Python](#python)
    - [Postgresql](#postgresql)
    - [URLs for API, S3 Buckets, etc.](#urls-for-api-s3-buckets-etc)
  - [Usage instructions](#usage-instructions)
  - [License](#license)


## Extracting and cleaning data

### Sources

The data comes from different sources. These include:

- AWS RDS Postgresql database
- Tables embedded in a PDF document
- AWS API for JSON data
- AWS S3 Bucket files (CSV, JSON)

### Methodology

There are three main classes to this program:

- **DatabaseConnector**: acts as a connection to a Postgresql database, allowing read/write of tables.
- **DataExtractor**: contains methods for extracting data from the sources, and returning pandas DataFrame objects.
- **DataCleaning**: contains methods to clean and format the data from DataExtractor so that it is ready to be uploaded to a local database.

Additionally, there is an `interactive.ipynb` notebook which demonstrates the process of interacting with the datasets to determine what functions were required to clean the data. This meant testing the results were much easier as the code could be executed step by step, and checking the results.

## Installation instructions

### Python

1. Ensure Python and Conda are installed.
2. Install the `conda` environment with `conda env create -f environment.yml`.
3. Activate the environment with `conda activate multinational-retail-data-centralisation874`

### Postgresql

1. Ensure you have a local Postgresql installation running.
2. Create a file called `local_db_creds.yaml`. Populate it with the following:

    ```yaml
    RDS_HOST: <host>
    RDS_USER: <username>
    RDS_PASSWORD: <password>
    RDS_DATABASE: <db to store data>
    RDS_PORT: <port>
    ```

    For example

    ```yaml
    RDS_HOST: localhost
    RDS_PASSWORD: 'my-very-strong-password'
    RDS_USER: postgres
    RDS_DATABASE: sales_data
    RDS_PORT: 5432
    ```
3. Repeat the above for a file called `db_creds.yaml`. This should contain the remote database details.

### URLs for API, S3 Buckets, etc.

- For the API credentials, see `api_creds.example.yml`. Make a copy and rename it to `api_creds.yml`. Then fill in the details.
- For S3 buckets, do the following:
    - For the Product data, create a text file called `product_bucket_url.txt` and paste the S3 URL for that file.
    - For the Date events data, create a text file called `date_bucket_url.txt` and paste the S3 URL for that file.
- For the PDF file, create a text file called `pdf_url.txt` and paste the URL for the file.

## Usage instructions

1. Open a terminal in the current working directory, and ensure Conda is activated.
2. Run `main.py` with the following: `python main.py`
3. This will run through fetching, cleaning, and uploading each data source to the local database instance.
4. The time for each step will be output into the console.

**Note**: Some of these operations can take a long time due to rate limits or large data sets.

## License

MIT License. See `LICENSE` for information.