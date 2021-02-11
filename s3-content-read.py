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


# for line in open('s3://jnicamzn-glacier-restore-manifests-2021/2021-02-03-restore-manifest.csv'):
#    print(repr(line))

# for line in open('/Users/jnicamzn/code/s3-batch-ops-restore-status-check/inventory-test-100.csv'):
#    print(repr(line))


""" import boto3
import gzip
import os
import csv


def is_s3_object(incoming):
    if incoming.lower().find("s3://", 0, 5) >= 0:
        return True
    else:
        return False


def split_s3_path(s3_path):
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket = path_parts.pop(0)
    key = "/".join(path_parts)
    return bucket, key


def get_extension(filename):
    ext = os.path.splitext(filename)[1][1:].strip().lower()
#    basename = os.path.basename(filename)  # os independent
#    ext = '.'.join(basename.split('.')[1:])
    return '.' + ext if ext else None


def read_s3_object(s3url):
    content = []

    if not is_s3_object(s3url):
        print("Not an S3 Object")
        quit()

    bucket, key = split_s3_path(s3url)

    ext = get_extension(key)

    obj = s3.Object(bucket,
                    key)

    if ext == ".gz":
        return gzip.GzipFile(fileobj=obj.get()["Body"])
#        with gzip.GzipFile(fileobj=obj.get()["Body"]) as gzipfile:
#            content = gzipfile.read()
    else:
        return obj.get()['Body']
#        content = obj.get()['Body'].read()


s3 = boto3.resource("s3")
s3url_gz = "s3://jnicamzn-glacier-restore-manifests-2021/2021-02-04-restore-manifest.csv.gz"
s3url_csv = "s3://jnicamzn-glacier-restore-manifests-2021/2021-02-04-restore-manifest.csv"

# ext = get_extension(key)

file_gz = read_s3_object(s3url_gz)


print("GZip Object")
print(file_gz.read())

file_csv = read_s3_object(s3url_gz)

print("Text Object")
print(file_csv.read())

print("GZip CSV")

with open(file_gz) as file:
    csv_reader = csv.reader(file, delimiter=",")
    for row in csv_reader:
        print(row[0])
        print(row[1])
 """
