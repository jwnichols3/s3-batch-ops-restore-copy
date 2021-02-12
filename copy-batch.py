import argparse
import boto3
import csv
from urllib.parse import unquote
import time
from smart_open import open

parser = argparse.ArgumentParser(
    description="Check on status of Glacier objects restored using S3 Batch Operations.")
parser.add_argument('--inventory_file', '-i', required=True,
                    help='The file that has a csv formatted list of inventory to check. The first column of the CSV is the bucket, the second column is the key. This can be an S3 object or local file. It can also be gzipped.')
parser.add_argument('--batchname', '-b', default="nobatchname")
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
print(f"Started at {time.strftime('%X', start_time)}")

batch_id = args.batchname
inventory_file = args.inventory_file
show = args.show
# last = args.last
dryrun = args.dryrun

object_list = []
response_list = []
object_count = 0
restore_complete_count = 0
restore_incomplete_count = 0
linecount = 0
starting_point = 0

logfile_id = time.strftime('%Y-%m-%d')
logfile_suffix = str(int(time.time()))
detail_file = "restore-check-" + batch_id + "-" + \
    logfile_id+"-"+logfile_suffix+"-detail.log"
summary_file = "restore-check-" + batch_id + "-" + \
    logfile_id+"-"+logfile_suffix+"-summary.log"
detail_f = open(detail_file, "a")
summary_f = open(summary_file, "a")

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

now = time.localtime()

detail_f.write(f"=== Started at {time.strftime('%X', start_time)}" + "\n")
summary_f.write(f"=== Started at {time.strftime('%X', start_time)}" + "\n")

if dryrun:
    print(f"********* DRY RUN **********")
    print(f"Objects will not be copied. Just the inventory items will be listed.")

print(f"Running...")
with open(inventory_file) as file:
    csv_reader = csv.reader(file, delimiter=",")
    current_row = 0
    for row in csv_reader:
        if current_row < starting_point:
            current_row += 1
            continue

        bucket = unquote(row[0])
        object = unquote(row[1])

        if dryrun:
            print(row)
            continue

        responsehead = s3_client.head_object(
            Bucket=bucket,
            Key=object
        )

        if (object_count % 10 == 0) and (object_count > 0):
            print(str(object_count) + " objects checked")
        if not responsehead:
            print("Does not exist: bucket = " + bucket + " object = " + object)
            detail_f.write("Object " + object + " does not exist.")
        else:
            object_count += 1
            if show:
                print("Object: " + object)
            detail_f.write("Object: " + object + "\n")
            response_http = responsehead['ResponseMetadata']['HTTPHeaders']
            if 'x-amz-restore' in response_http:
                restore_response = response_http['x-amz-restore']
                restore_status = restore_response.split(", ", 1)[0]
                restore_expiry = restore_response.split(", ", 1)[1]

                restore_ongoing = restore_status.split("=")[1]
                if restore_ongoing == '"false"':
                    restore_complete_count = restore_complete_count + 1
                else:
                    restore_incomplete_count = restore_incomplete_count + 1

#                restore_status = restore_response.split()
#                restore_expiry = restore_response[1]
#                print("GLACIER RESTORE! Restore Status: " + restore_response)
            for a, b in response_http.items():
                # print(a, b)
                detail_f.write(a + " " + b + ", ")
            detail_f.write("\n")

end_time = time.localtime()
time_diff = time.mktime(end_time) - time.mktime(start_time)

detail_f.write(f"Objects traversed: " + str(object_count) + "\n")
detail_f.write(f"Objects restore complete: " +
               str(restore_complete_count) + "\n")
detail_f.write(f"Objects restore incomplete: " +
               str(restore_incomplete_count) + "\n")
detail_f.write(f"=== Ended at {time.strftime('%X', end_time)}" + "\n")
detail_f.write(f"Total time: " + str(time_diff) + " seconds\n")

summary_f.write(f"Objects traversed: " + str(object_count) + "\n")
summary_f.write(f"Objects restore complete: " +
                str(restore_complete_count) + "\n")
summary_f.write(f"Objects restore incomplete: " +
                str(restore_incomplete_count) + "\n")
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
print(f"Objects restore complete: " + str(restore_complete_count))
print(f"Objects restore incomplete: " + str(restore_incomplete_count))
print(f"Summary Log: " + summary_file)
print(f"Detail Log:  " + detail_file)

detail_f.close()
detail_f.close()
