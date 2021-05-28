import requests
import time
from typing import Tuple
from faker import Faker
from collections import defaultdict
from datetime import datetime, date, timedelta
from aws_lambda_powertools import Logger

import boto3
import requests
import csv
import io

s3 = boto3.client('s3')
logger = Logger()
faker = Faker()
today = date.today()


@logger.inject_lambda_context
def handler(event, context):

    object_get_context = event["getObjectContext"]
    request_route = object_get_context["outputRoute"]
    request_token = object_get_context["outputToken"]
    s3_url = object_get_context["inputS3Url"]


# Get original object (not anonymized) from S3    
    original_object = download_file_from_s3("https://******.s3.eu-central-1.amazonaws.com/patients.csv")


# Anonymize    
    anonymized_object = anonymize(original_object)
    with open('newfile.txt', 'w', encoding='utf-8') as g:
        g.write(anonymized_object)


# Send response back to requester
    s3.write_get_object_response(
    Body=anonymized_object,
    RequestRoute=request_route,
    RequestToken=request_token)


    return {'status_code': 200}

def download_file_from_s3(presigned_url):
    response = requests.get(presigned_url)
    if response.status_code != 200:
        logger.error("Failed to download original file from S3")
        raise Exception(f"Failed to download original file from S3, error {response.status_code}")

    return response.content.decode('utf-8-sig')


def filter_columns(reader, keys):
    for r in reader:
        yield dict((k, r[k]) for k in keys)


def anonymize(original_object)-> Tuple[int, str]:
    logger.debug("Anonymizing object")
    reader = csv.DictReader(io.StringIO(original_object))

    input_selected_fieldnames = ['Fullname','Phone','Birthdate','Gender','Smoking','Weight','Height','Disease','Address','Email']
    output_selected_fieldnames = input_selected_fieldnames.copy()
    transformed_object = ''
    with io.StringIO() as output:
        writer = csv.DictWriter(output, fieldnames=output_selected_fieldnames, quoting=csv.QUOTE_NONE)
        writer.writeheader()
        rows = 0
        for row in filter_columns(reader, input_selected_fieldnames):
            writer.writerow(anonymize_email(row))
            rows += 1

        transformed_object = output.getvalue()
    print(transformed_object)
    return rows, transformed_object

def anonymize_email(row):
    anonymized_row = row.copy()
    anonymized_row['Email'] = faker.safe_email()
    return anonymized_row


