import argparse
import boto3
from botocore.exceptions import ClientError
import csv
from urllib.parse import unquote
import time
from smart_open import open

parser = argparse.ArgumentParser(
    description="Check on status of Glacier objects restored using S3 Batch Operations.")
parser.add_argument('--inventory_file', '-i', required=True,
                    help='The file that has a csv formatted list of inventory to check. The first column of the CSV is the bucket, the second column is the key. This can be an S3 object or local file. It can also be gzipped.')
parser.add_argument('--batchname', '-b', default="nobatchname",
                    help="Use batchname as a way to name the run of the script for logging purposes.")
parser.add_argument(
    '--show', action='store_true', help='This will show the list of files as they are checked.')
parser.add_argument(
    "--last", type=int, help='This will run the check on the last # objects in the inventory'
)
parser.add_argument(
    "--dryrun", action='store_true', help='Do not run the S3 API call, just list the inventory items.'
)
parser.add_argument(
    "--profile", help='Use a specific AWS Profile'
)

args = parser.parse_args()
start_time = time.localtime()
batch_id = args.batchname
inventory_file = args.inventory_file
show = args.show
last = args.last
dryrun = args.dryrun
profile = args.profile
object_list = []
response_list = []
object_count = 0
restore_complete_count = 0
restore_incomplete_count = 0
objecthead_error_count = 0
linecount = 0
starting_point = 0
now = time.localtime()
logfile_id = time.strftime('%Y-%m-%d')
logfile_epoch_suffix = str(int(time.time()))
logfile_suffix = batch_id + "-" + logfile_id + "-" + logfile_epoch_suffix
detail_file = "restore-check-" + logfile_suffix + "-detail.log"
summary_file = "restore-check-" + logfile_suffix + "-summary.log"
inventory_report_csv = "restore-check-inventory-report-" + logfile_suffix + ".csv"
# If a profile is specified, use it (error out if the profile isn't found)
if profile:
    try:
        boto3.setup_default_session(profile_name=profile)
    except Exception as err:
        print(err)
        exit()

s3_client = boto3.client('s3')
detail_f = open(detail_file, "a")
summary_f = open(summary_file, "a")
inventory_report_f = open(inventory_report_csv, "a")

print(f"Started at {time.strftime('%X', start_time)}")
detail_f.write(f"=== Started at {time.strftime('%X', start_time)}" + "\n")
summary_f.write(f"=== Started at {time.strftime('%X', start_time)}" + "\n")

if last:  # if the --last option is passed
    print(f"Analyzing inventory file...")
    linecount = len(open(inventory_file).readlines())

    starting_point = linecount - last

    if starting_point < 0:
        print("ERROR The inventory file has " + str(linecount) +
              " lines. The last variable of " + str(last) + " must be equal or less than that.")
        quit()

    print("Number of lines in the " + inventory_file +
          " inventory file: " + str(linecount))
    print("Show the last " + str(last) + " lines.")
    print("Starting point: " + str(starting_point))

    summary_f.write(f"Number of lines in the " + inventory_file + " inventory file: " + str(linecount) + "\n" +
                    "Show the last " +
                    str(last) + " lines starting at " +
                    str(starting_point) + "\n"
                    )

if dryrun:  # if --dryrun is passed
    print(f"********* DRY RUN **********")
    print(f"Objects will not be queried. Just the inventory items will be listed.")

#################################################
print(f"Running...")
with open(inventory_file) as file:
    csv_reader = csv.reader(file, delimiter=",")
    current_row = 0
    for row in csv_reader:
        # For the --last option
        # Skip the record if not one of the last lines.
        if current_row < starting_point:  # this is --last logic
            current_row += 1
            continue

        bucket = unquote(row[0])
        object = unquote(row[1])

        if dryrun:
            print(row)
            continue

        try:
            responsehead = s3_client.head_object(
                Bucket=bucket,
                Key=object
            )
        except ClientError as e:
            print("ERROR " + e.response['Error']['Code'] +
                  " on Bucket: " + bucket + " / Object: " + object)
            detail_f.write("Error " + e.response['Error']['Code'] +
                           " on Bucket: " + bucket + " / Object: " + object)
            objecthead_error_count += 1
            object_count += 1
            continue

        if (object_count % 10 == 0) and (object_count > 0):
            print(str(object_count) + " objects checked")

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
                inventory_report_f.write(
                    "\'" + object + "\',\'" + restore_expiry + "\'\n")
            else:
                restore_incomplete_count = restore_incomplete_count + 1

        for a, b in response_http.items():
            # print(a, b)
            detail_f.write(a + " " + b + ", ")

        detail_f.write("\n")

#################################################

end_time = time.localtime()
time_diff = time.mktime(end_time) - time.mktime(start_time)

#### Detail Logging ####
detail_f.write(f"Objects traversed: " + str(object_count) + "\n")
detail_f.write(f"Objects restore complete: " +
               str(restore_complete_count) + "\n")
detail_f.write(f"Objects restore incomplete: " +
               str(restore_incomplete_count) + "\n")
detail_f.write(f"Objects with errors: " +
               str(objecthead_error_count) + "\n")
detail_f.write(f"=== Ended at {time.strftime('%X', end_time)}" + "\n")
detail_f.write(f"Total time: " + str(time_diff) + " seconds\n")

#### Summary Logging ####
summary_f.write(f"Objects traversed: " + str(object_count) + "\n")
summary_f.write(f"Objects restore complete: " +
                str(restore_complete_count) + "\n")
summary_f.write(f"Objects restore incomplete: " +
                str(restore_incomplete_count) + "\n")
summary_f.write(f"Objects with errors: " +
                str(objecthead_error_count) + "\n")
summary_f.write(f"=== Ended at {time.strftime('%X', end_time)}" + "\n")
summary_f.write(f"Total time: " + str(time_diff) + " seconds\n")

#### Screen Output ###
if object_count > 0:
    time_per_object = time_diff / object_count
else:
    time_per_object = 0

print(f"Ended at {time.strftime('%X', end_time)}")
print(f"Total time: " + str(time_diff) + " seconds")
print(f"Avg per object: " + str(time_per_object))
print(f"Total objects: " + str(object_count))
print(f"Objects restore complete: " + str(restore_complete_count))
print(f"Objects restore incomplete: " + str(restore_incomplete_count))
print(f"Objects with errors: " +
      str(objecthead_error_count) + "\n")
print(f"Summary Log:                 " + summary_file)
print(f"Detail Log:                  " + detail_file)
print(f"Restore Check Inventory CSV: " + inventory_report_csv)

detail_f.close()
detail_f.close()
inventory_report_f.close()
