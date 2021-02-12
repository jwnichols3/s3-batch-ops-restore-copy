import boto3
import csv
from urllib.parse import unquote
import time
from smart_open import open

s3 = boto3.resource('s3')

target_bucket = "jnicamzn-glacier-copy-test-root"

with open('s3://jnicamzn-glacier-restore-manifests-2021/2021-02-11-restore-manifest-all.csv.gz') as file:
    #    print(repr(line))
    csv_reader = csv.reader(file, delimiter=",")
    current_row = 0
    for row in csv_reader:
        bucket = unquote(row[0])
        object = unquote(row[1])

        copy_source = {
            'Bucket': bucket,
            'Key': object
        }

        print("Copying " + object)
        s3.meta.client.copy(copy_source, target_bucket, object)
