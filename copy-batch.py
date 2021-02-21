import argparse
import boto3
from botocore.exceptions import ClientError
import csv
from urllib.parse import unquote
import time
from smart_open import open
import os

parser = argparse.ArgumentParser(
    description="Copy large S3 objects from one S3 bucket to another based on a manifest file.")
parser.add_argument('--inventory_file', '-i', required=True,
                    help='The file that has a csv formatted list of inventory to check. The first column of the CSV is the bucket, the second column is the key. This can be an S3 object or local file. It can also be gzipped.')
parser.add_argument('--target_bucket', required=True,
                    help="This is the target bucket for the objects. This script assumes you have bucket policies and associated rights to access")
parser.add_argument('--batchname', '-b', default="nobatchname",
                    help="Use batchname as a way to name the run of the script for logging purposes.")
parser.add_argument(
    '--show', action='store_true', help='This will show the list of files as they are checked.')
parser.add_argument(
    '--env', action='store_true', help="use the AWS environment variables for aws_access_key_id and aws_secret_access_key values")
parser.add_argument(
    "--profile", help='Use a specific AWS Profile'
)
parser.add_argument(
    "--dryrun", action='store_true', help='Do not run the S3 API call, just list the inventory items.'
)

args = parser.parse_args()
start_time = time.localtime()
batch_id = args.batchname
inventory_file = args.inventory_file
show = args.show
dryrun = args.dryrun
env = args.env
profile = args.profile
target_bucket = args.target_bucket
object_list = []
response_list = []
object_count = 0
copy_complete_count = 0
copy_incomplete_count = 0
copy_error_count = 0
total_records = 0
starting_point = 0
total_bytes = 0
logfile_id = time.strftime('%Y-%m-%d')
logfile_suffix = str(int(time.time()))
detail_file = "copy-batch-" + batch_id + "-" + \
    logfile_id+"-"+logfile_suffix+"-detail.log"
summary_file = "copy-batch-" + batch_id + "-" + \
    logfile_id+"-"+logfile_suffix+"-summary.log"

# I'm sure there is a way to do this more elegantly...
# First priority: If --env is specified, use the environment variables
# Second priority: if --profile is specified, use the profile name
# Last priority: if nothing is specified, use the current user
if env:
    try:
        boto3.setup_default_session(
            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
        )
        s3_client = boto3.client(
            's3'
        )
        s3 = boto3.resource('s3',
                            aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
                            )
    except Exception as err:
        print(err)
        exit()
elif profile:
    boto3.setup_default_session(profile_name=profile)
    s3_client = boto3.client('s3')
    s3 = boto3.resource('s3')
else:
    s3_client = boto3.client('s3')
    s3 = boto3.resource('s3')


now = time.localtime()
detail_f = open(detail_file, "a")
summary_f = open(summary_file, "a")

start_time_log = "=== Started at " + str(time.strftime('%X', start_time))
print(start_time_log)
detail_f.write(start_time_log + "\n")
summary_f.write(start_time_log + "\n")

print(f"Analyzing inventory file...")
total_records = len(open(inventory_file).readlines())

log_msg = "Number of lines in the " + inventory_file + \
    " inventory file: " + str(total_records)

print(log_msg)
summary_f.write(log_msg + "\n")
detail_f.write(log_msg + "\n")

if dryrun:
    print(f"********* DRY RUN **********")
    print(f"Objects will not be copied. Just the inventory items will be listed.")

#################################################
print(f"Running...")
with open(inventory_file) as file:
    csv_reader = csv.reader(file, delimiter=",")
    current_row = 0

    for row in csv_reader:
        if dryrun:
            print(row)
            continue

        bucket = unquote(row[0])
        object = unquote(row[1])

        copy_source = {
            'Bucket': bucket,
            'Key': object
        }

        object_count += 1
        perc_complete = object_count / total_records
        perc_complete_str = "Percent complete: " + \
            "{:.2%}".format(perc_complete)

        print(time.strftime("%Y-%m-%d %H:%M:%S",
                            time.localtime()) + " - " + str(object_count) + " of " + str(total_records) + " - " + object + " - start.")
        now = time.time()

        try:
            resp = s3_client.head_object(
                Bucket=bucket,
                Key=object
            )
            obj_size = resp['ContentLength']
            s3.meta.client.copy(copy_source, target_bucket, object,
                                ExtraArgs={
                                    'StorageClass': 'INTELLIGENT_TIERING'
                                })

        except ClientError as e:
            error_line = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + " ERROR " + \
                e.response['Error']['Code'] + " on Bucket: " + \
                bucket + " / Object: " + object
            print(error_line)
            detail_f.write(error_line)
            copy_error_count += 1
            continue

        later = time.time()
        time_difference = int(later - now)
        total_bytes = total_bytes + obj_size

        if time_difference == 0:
            time_difference = 1

        bytes_in = obj_size / time_difference
        bytes_in_MB = bytes_in / 1024

        bytes_in_str = "Copied " + str(obj_size) + " in " + str(
            time_difference) + " sec. Rate: " + "{:.2f}".format(bytes_in_MB) + "MB per sec."

        logging_line = time.strftime("%Y-%m-%d %H:%M:%S",
                                     time.localtime()) + " - " + str(object_count) + " of " + str(total_records) + " - " + object + " - end.   " + bytes_in_str + " " + perc_complete_str

        print(logging_line)
        detail_f.write(logging_line + "\n")
        copy_complete_count += 1

end_time = time.localtime()
time_diff = time.mktime(end_time) - time.mktime(start_time)

if time_diff == 0:
    time_diff = 1

total_bytes_MB = total_bytes / 1024
total_bytes_GB = total_bytes_MB / 1024
total_rate_GB = total_bytes_GB / time_diff

if object_count > 0:
    time_per_object = time_diff / object_count
else:
    time_per_object = 0

summary_log = \
    "=== Ended at " + str(time.strftime('%X', end_time)) + "\n" + \
    "Copied " + "{:.2f}".format(total_bytes_GB) + "GB total at a rate of " + "{:.2f}".format(total_rate_GB) + "GB per sec.\n" + \
    "Objects traversed: " + str(object_count) + "\n" + \
    "Objects copied: " + str(copy_complete_count) + "\n" + \
    "Objects failed: " + str(copy_error_count) + "\n" + \
    "Total time: " + str(time_diff) + " seconds\n" + \
    "Avg per object: " + str(time_per_object)

detail_f.write(summary_log)
summary_f.write(summary_log)

# Screen Output
print(summary_log)
print(f"Summary Log: " + summary_file)
print(f"Detail Log:  " + detail_file)

detail_f.close()
detail_f.close()
