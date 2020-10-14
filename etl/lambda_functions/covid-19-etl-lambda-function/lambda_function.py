import sys
sys.path.append('etl')
import boto3
import json
import os

from helpers.env_helper import EnvHelper
from helpers.csv_helper import CsvHelper
from data_accessors.covid_19_data_accessor import Covid19DataAccessor

env_helper = EnvHelper()
csv_helper = CsvHelper()
covid_19_data_accessor = Covid19DataAccessor(env_helper)

def lambda_handler(event, context):
    
    print("Getting John Hopkins data")
    jh_url = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"
    jh_csv = csv_helper.get_csv_from_url(jh_url)

    print("Getting NYT data")
    nyt_url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
    nyt_csv = csv_helper.get_csv_from_url(nyt_url)

    print("Validating data sets")
    is_jh_valid = covid_19_data_accessor.validate_jh(jh_csv)
    is_nyt_valid = covid_19_data_accessor.validate_nyt(nyt_csv)

    if is_jh_valid and is_nyt_valid:

        print("Formatting data sets")
        jh_csv_us = csv_helper.filter_by_column_value(jh_csv, 'Country/Region', 'US')
        jh_csv_us = csv_helper.format_date(jh_csv_us, 'Date')
        nyt_csv = csv_helper.format_date(nyt_csv, 'date')
        
        print("Combining data sets")
        combined_csv = nyt_csv.merge(jh_csv_us.filter(items=['Date', 'Recovered']), how='left', left_on='date', right_on='Date').filter(items=['date', 'cases', 'deaths', 'Recovered'])
        combined_csv = combined_csv[combined_csv['Recovered'].notnull()] # remove rows that did not exist in both data sets

        print("Getting current data in dynamodb")
        current_entries = covid_19_data_accessor.get_all_covid_19_entries()

        print("Remove entries that already exist in the database from the combined data")
        if current_entries is not None:
            combined_csv = csv_helper.purge(combined_csv, 'date', current_entries, 'date')

        print("Upload data to dynamodb")
        total_uploads = covid_19_data_accessor.load_all_covid_19_row_data(combined_csv)

        print("Publish to upload complete SNS topic")
        message = {"totalUploads": total_uploads}
        client = boto3.client('sns')
        response = client.publish(
            TargetArn=os.getenv('CovidUploadComplete'),
            Message=json.dumps(message)
        )

        return
    else:
        message = {"error": "data sets were invalid"}
        client = boto3.client('sns')
        response = client.publish(
            TargetArn=os.getenv('CovidUploadComplete'),
            Message=json.dumps(message)
        )
        return

if __name__ == "__main__":
    lambda_handler(None, None)