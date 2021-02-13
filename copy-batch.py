import argparse
import boto3
from botocore.exceptions import ClientError
import csv
from urllib.parse import unquote
import time
from smart_open import open

s3 = boto3.resource('s3')

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
# parser.add_argument(
#    "--last", type=int, help='This will run the check on the last # objects in the inventory'
# )
parser.add_argument(
    "--dryrun", action='store_true', help='Do not run the S3 API call, just list the inventory items.'
)

args = parser.parse_args()
start_time = time.localtime()
batch_id = args.batchname
inventory_file = args.inventory_file
show = args.show
dryrun = args.dryrun
target_bucket = args.target_bucket
object_list = []
response_list = []
object_count = 0
copy_complete_count = 0
copy_incomplete_count = 0
copy_error_count = 0
linecount = 0
starting_point = 0
logfile_id = time.strftime('%Y-%m-%d')
logfile_suffix = str(int(time.time()))
detail_file = "copy-batch-" + batch_id + "-" + \
    logfile_id+"-"+logfile_suffix+"-detail.log"
summary_file = "copy-batch-" + batch_id + "-" + \
    logfile_id+"-"+logfile_suffix+"-summary.log"
detail_f = open(detail_file, "a")
summary_f = open(summary_file, "a")
s3_client = boto3.client('s3')

now = time.localtime()

print(f"Started at {time.strftime('%X', start_time)}")
detail_f.write(f"=== Started at {time.strftime('%X', start_time)}" + "\n")
summary_f.write(f"=== Started at {time.strftime('%X', start_time)}" + "\n")

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

        print("Count: " + str(object_count))
        print(time.strftime("%Y-%m-%d, %H:%M:%S",
                            time.localtime()) + " - " + object + " - start")
        now = time.time()

        try:
            s3.meta.client.copy(copy_source, target_bucket, object)

        except ClientError as e:
            print("ERROR " + e.response['Error']['Code'] +
                  " on Bucket: " + bucket + " / Object: " + object)
            detail_f.write("Error " + e.response['Error']['Code'] +
                           " on Bucket: " + bucket + " / Object: " + object)
            copy_error_count += 1
            continue

        print(time.strftime("%Y-%m-%d, %H:%M:%S",
                            time.localtime()) + " - " + object + " - end")
        later = time.time()
        time_difference = int(later - now)
        print("Time to copy: " + str(time_difference))

        detail_f.write(time.strftime("%Y-%m-%d, %H:%M:%S",
                                     time.localtime()) + " - " + object + " in " + str(time_difference) + " seconds.")
        copy_complete_count += 1

end_time = time.localtime()
time_diff = time.mktime(end_time) - time.mktime(start_time)

# To do here: logging


detail_f.write(f"Objects traversed: " + str(object_count) + "\n")
detail_f.write(f"Objects copied: " +
               str(copy_complete_count) + "\n")
detail_f.write(f"Objects failed: " +
               str(copy_error_count) + "\n")
detail_f.write(f"=== Ended at {time.strftime('%X', end_time)}" + "\n")
detail_f.write(f"Total time: " + str(time_diff) + " seconds\n")

summary_f.write(f"Objects traversed: " + str(object_count) + "\n")
summary_f.write(f"Objects copied: " +
                str(copy_complete_count) + "\n")
summary_f.write(f"Objects failed: " +
                str(copy_error_count) + "\n")
summary_f.write(f"=== Ended at {time.strftime('%X', end_time)}" + "\n")
summary_f.write(f"Total time: " + str(time_diff) + " seconds\n")

if object_count > 0:
    time_per_object = time_diff / object_count
else:
    time_per_object = 0

# Screen Output
print(f"Ended at {time.strftime('%X', end_time)}")
print(f"Total time: " + str(time_diff) + " seconds")
print(f"Avg per object: " + str(time_per_object))
print(f"Total objects: " + str(object_count))
print(f"Objects copied: " + str(copy_complete_count))
print(f"Objects failed: " + str(copy_error_count))
print(f"Summary Log: " + summary_file)
print(f"Detail Log:  " + detail_file)

detail_f.close()
detail_f.close()
