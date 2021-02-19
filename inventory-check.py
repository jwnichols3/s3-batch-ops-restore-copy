import argparse
import boto3
from botocore.exceptions import ClientError
from urllib.parse import unquote
import time
from smart_open import open
import os
import sys

s3 = boto3.resource('s3')

parser = argparse.ArgumentParser(
    description="Analyze Inventory Files")
parser.add_argument('--inventory_file', '-i',
                    help='The file that has a csv formatted list of inventory to check. The first column of the CSV is the bucket, the second column is the key. This can be an S3 object or local file. It can also be gzipped.')
parser.add_argument('--inventory_directory',
                    help='A directory with a set of inventories. this will recursively iterate across all folders/files.')
parser.add_argument(
    '--env', action='store_true', help="use the AWS environment variables for aws_access_key_id and aws_secret_access_key values")
parser.add_argument(
    "--profile", help='Use a specific AWS Profile'
)

args = parser.parse_args()
start_time = time.localtime()
inventory_file = args.inventory_file
inventory_directory = args.inventory_directory
env = args.env
profile = args.profile
object_list = []
response_list = []
object_count = 0
total_records = 0

if (not inventory_file) and (not inventory_directory):
    print("--inventory_file or --inventory_directory is required")
    exit()


# I'm sure there is a way to do this more elegantly...
# First priority: If --env is specified, use the environment variables
# Second priority: if --profile is specified, use the profile name
# Last priority: if nothing is specified, use the current user
if env:
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
        )
        print(os.environ)
    except Exception as err:
        print(err)
        exit()
elif profile:
    boto3.setup_default_session(profile_name=profile)
    s3_client = boto3.client('s3')
else:
    s3_client = boto3.client('s3')


if inventory_file:
    print(f"Analyzing inventory file...")
    total_records = len(open(inventory_file).readlines())

    print("Number of records in the " + inventory_file +
          " inventory file: " + str(total_records))

if inventory_directory:
    print("Walking directory " + os.path.abspath(inventory_directory))

    for dirpath, dirs, files in os.walk(inventory_directory):

        for f in files:
            inv_file = dirpath + "/" + f
            # print("Inv File: " + inv_file)
            records_files = len(open(inv_file).readlines())
            # print("Num records in " + inv_file)
            print(f + ": " + str(records_files))
