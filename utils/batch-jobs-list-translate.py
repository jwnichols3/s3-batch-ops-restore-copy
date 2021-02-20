import argparse
import boto3
from botocore.exceptions import ClientError
import csv
from urllib.parse import unquote
import time
from smart_open import open
import json
from datetime import datetime

parser = argparse.ArgumentParser(
    description="Copy large S3 objects from one S3 bucket to another based on a manifest file.")
parser.add_argument('--report_file', '-i', required=True,
                    help='This is a JSON formatted file with output from the AWS CLI command `aws s3control list-jobs` - This file can be an S3 object or local file.')


def utc2local(utc):
    epoch = time.mktime(utc.timetuple())
    offset = datetime.fromtimestamp(epoch) - datetime.utcfromtimestamp(epoch)
    return utc + offset


args = parser.parse_args()
report_file = args.report_file

with open(report_file) as f:
    data = json.load(f)

for i in data['Jobs']:
    jobid = i['JobId']
    name = i['Description']
    operation = i['Operation']
    status = i['Status']
    progress = i['ProgressSummary']
    createtimez = i['CreationTime']
    ct_zulu = datetime.fromisoformat(createtimez[:-1])
    ct_local = utc2local(ct_zulu)
    createtime = ct_local.strftime('%Y-%m-%d %H:%M:%S')
    createtime_zulu = ct_zulu.strftime('%Y-%m-%d %H:%M:%S')
    print("Name: " + name + ", " +
          " JobId: " + jobid + ", " +
          " Status: " + status + ", " +
          " Start (local): " + createtime + ", " +
          " Op: " + operation)


print("End")
