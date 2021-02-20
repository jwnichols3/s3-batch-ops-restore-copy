import argparse
import csv
from urllib.parse import unquote
import time

parser = argparse.ArgumentParser(
    description="Check on status of Glacier objects restored using S3 Batch Operations.")
parser.add_argument('--inventory_file', '-i', required=True,
                    help='The file that has a csv formatted list of inventory to check. The first column of the CSV is the bucket, the second column is the key.')
parser.add_argument('--batchname', '-b', default="nobatchname")
parser.add_argument(
    '--show', action='store_true', help='This will show the list of files as they are checked.')
parser.add_argument(
    "--last", type=int, help='This will run the check on the last # objects in the inventory'
)

args = parser.parse_args()
start_time = time.localtime()
print(f"Started at {time.strftime('%X', start_time)}")

batch_id = args.batchname
inventory_file = args.inventory_file
show = args.show
last = args.last

object_list = []
response_list = []
object_count = 0
restore_complete_count = 0
restore_incomplete_count = 0
linecount = 0
starting_point = 0

now = time.localtime()

print(f"Analyzing inventory file...")
if last:
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

print(f"Running...")
with open(inventory_file) as file:
    csv_reader = csv.reader(file, delimiter=",")
    current_row = 0
    for row in csv_reader:

        if current_row < starting_point:
            current_row += 1
            continue

        print("Current row:" + str(current_row))
        current_row += 1
        object_count += 1
        print(row)


end_time = time.localtime()
time_diff = time.mktime(end_time) - time.mktime(start_time)

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
