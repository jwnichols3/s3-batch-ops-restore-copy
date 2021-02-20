import argparse
import boto3
import csv
from urllib.parse import unquote
import time
from smart_open import open

with open('s3://jnicamzn-glacier-restore-manifests-2021/2021-02-04-restore-manifest.csv.gz') as file:
    #    print(repr(line))
    csv_reader = csv.reader(file, delimiter=",")
    current_row = 0
    for row in csv_reader:
        bucket = unquote(row[0])
        object = unquote(row[1])
        print("Bucket: " + bucket + " / Object: " + object)
