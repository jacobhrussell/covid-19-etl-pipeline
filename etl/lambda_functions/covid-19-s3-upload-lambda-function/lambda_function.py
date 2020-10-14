import sys
sys.path.append('etl')
import boto3
import json
from helpers.env_helper import EnvHelper
from data_accessors.covid_19_data_accessor import Covid19DataAccessor

env_helper = EnvHelper()
covid_19_data_accessor = Covid19DataAccessor(env_helper)

def lambda_handler(event, context):

    print("Get entries in dynamodb")
    entries = covid_19_data_accessor.get_all_covid_19_entries()

    print("Converting timestamps")
    for entry in entries:
        entry['date'] = str(entry['date'])

    print("Get s3 bucket")
    s3 = boto3.resource('s3', )
    bucket_name = env_helper.get_env() + '-covid-19-bucket'
    bucket = s3.Bucket(bucket_name)

    print("Deleting old objects in the bucket...")
    s3.Object(bucket_name, 'covid19.json').delete()

    print("Putting dictionary in the bucket...")
    s3_object = s3.Object(bucket_name, 'covid19.json')
    s3_object.put(Body=bytes(json.dumps(entries).encode('utf-8')))

    return

if __name__ == "__main__":
    lambda_handler(None, None)