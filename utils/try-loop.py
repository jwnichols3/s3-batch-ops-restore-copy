import argparse
from botocore.exceptions import ClientError
import boto3
import logging
import csv
from urllib.parse import unquote
import time
from smart_open import open

parser = argparse.ArgumentParser(
    description="A try-loop example.")
parser.add_argument('--inventory_file', '-i', required=True,
                    help='The file that has a csv formatted list of inventory to check. The first column of the CSV is the bucket, the second column is the key. This can be an S3 object or local file. It can also be gzipped.')

args = parser.parse_args()
inventory_file = args.inventory_file

s3_client = boto3.client('s3')


start_time = time.localtime()
print(f"Started at {time.strftime('%X', start_time)}")


with open(inventory_file) as file:
    csv_reader = csv.reader(file, delimiter=",")

    for row in csv_reader:

        bucket = unquote(row[0])
        object = unquote(row[1])

        try:
            responsehead = s3_client.head_object(
                Bucket=bucket,
                Key=object
            )
        except ClientError as e:
            print("ERROR " + e.response['Error']['Code'] +
                  " on Bucket: " + bucket + " / Object: " + object)
            continue

        print(responsehead)
