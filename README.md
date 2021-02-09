# AWS S3 Batch Operations launched Restore Status Check

## Requirements

* AWS SDK installed (boto3)
* Profile setup or running using a Role with access to the buckets/objects in question
* Inventory file with at least two values per line:
  * first column: bucket name
  * second collumn: key name

## Arguments

* --inventory_file - this is the csv file with objects to check.
* --batchname - a friendly name for the inventory name (restore jobs are often run in batches)
* --show - a flag to show the list of objects to the console as they are run.

## Example
```
python restore-check.py --inventory_file inventory-test-100.csv --batchname batch01
```