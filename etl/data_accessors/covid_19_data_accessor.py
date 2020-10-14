import boto3
import botocore
import json
import pandas as pd
import os

class Covid19DataAccessor:

    client = None
    env_helper = None

    def __init__(self, env_helper):
        self.client = boto3.client(
            'dynamodb'
        )
        self.env_helper = env_helper

    def get_all_covid_19_entries(self):
        response = self.client.scan(
            TableName=self.get_table_name(),
            Select='ALL_ATTRIBUTES'
        )
        formatted_response = self.format_dynamodb_data(response)
        return formatted_response
        
    def format_dynamodb_data(self, response):
        to_return = []
        for item in response['Items']:
            my_item = {}
            my_item['date'] = pd.to_datetime(item['myDate']['S'])
            my_item['cases'] = int(item['cases']['N'])
            my_item['deaths'] = int(item['deaths']['N'])
            my_item['recovered'] = float(item['recovered']['N'])
            to_return.append(my_item)
        return to_return

    def load_all_covid_19_row_data(self, all_data):
        count = 0
        for row in all_data.itertuples():
            self.load_covid_19_row_data(row)
            count = count + 1
        return count

    def load_covid_19_row_data(self, data):
        try:
            self.client.update_item(
                TableName=self.get_table_name(),
                Key={
                    'myDate': {'S': str(data.date)}
                },
                UpdateExpression='''
                    SET
                    cases = :cases,
                    deaths = :deaths,
                    recovered = :recovered
                    ''',
                ConditionExpression='''
                    myDate <> :myDate OR
                    cases <> :cases OR
                    deaths <> :deaths OR
                    recovered <> :recovered
                    ''',
                ExpressionAttributeValues={
                    ':myDate': {'S': str(data.date)},
                    ':cases': {'N': str(data.cases)},
                    ':deaths': {'N': str(data.deaths)},
                    ':recovered': {'N': str(data.Recovered)}
                }
            )
        except botocore.exceptions.ClientError as e:
            self.handle_error(e)
    
    def get_table_name(self):
        env = self.env_helper.get_env()
        table_name = env + '-covid-19-table'
        return table_name
    
    def handle_error(self, e):
        print("Upload failed. Posting error to SNS.")
        message = {"error": e}
        client = boto3.client('sns')
        response = client.publish(
            TargetArn=os.getenv('CovidUploadComplete'),
            Message=json.dumps(message)
        )

    def validate_jh(self, jh_data):
        columns = jh_data.columns
        if 'Recovered' not in columns and 'Date' not in columns:
            return False
        else:
            return True

    def validate_nyt(self, nyt_data):
        columns = nyt_data.columns
        if 'cases' not in columns and 'deaths' not in columns and 'date' not in columns:
            return False
        else:
            return True