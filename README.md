# AWS S3 Batch Operations launched Restore Status Check

For [batch-copy.py README](README_copy-batch.md)

Github Repo: https://github.com/jwnichols3/s3-batch-ops-restore-status-check

Why do you need this? 

S3 Batch Operations Restore Job -> issues a set of Glacier async restore commands for a manifest (usually based on an Inventory).

When the S3 Batch Operations Restore Job finishes, the Glacier object restore process can take 24-48 hours.

When the Glacier object restore process completes, the objects in S3 are still listed as in "GLACIER" or "GLACIER DEEP ARCHIVE"

You can navigate to the Console and check the "Restore progress" field and the "Expiry Date" field to see how long the file will be available.

This is a way to programaticaly retrieve the restore status and expiry date for all objects in a manifest. 

Update: this is an incorrect assumption
~~One assumption we are making is that the objects are processed by Glacier in the order of the manifest file. To simplify the checking of large restore jobs (100s of thousands of object or more), this program has a way to check the last # records (`--last #` option).~~
The Glacier service may process objects out of sequence for a variety of reasons (retry, queueing, etc)

There are two logfiles produced:
* `restore-check-BATCHNAME-YYYY-MM-DD-EPOCH-detail.log`
* `restore-check-BATCHNAME-YYYY-MM-DD-EPOCH-summary.log`
  
`BATCHNAME` is the value entered on the command line.

`YYYY-MM-DD-EPOCH` is the year, month, day and EPOCH time of the batch job. This makes the log filenamnes readable and unique.

## Disclaimers
Note: this is a personal project and not meant for production use. It is on you to review for your environment. I welcome feedback.

## Requirements

* AWS SDK installed (`pip install boto3`)
* Python3 smart_open module (`pip install smart_open`)
* Python3 with modules: argparse, csv, unquote, time
* AWS Profile setup or running using a Role with access to the buckets/objects in question
* Inventory file with at least two values per line:
  * first column: bucket name
  * second collumn: key name
  * [Example CSV File](inventory-example.csv)

## Arguments

* `--inventory_file {filename | S3 object URL in s3:// format}` - this is the csv inventory file with objects to check. See above for the inventory format. This can be a text CSV file or can be GZipped (.csv.gz).
* `--batchname {name}` - a friendly name for the inventory name (restore jobs are often run in batches).
* `--last #` - only process the last # entries in the Inventory file.
* `--show` - a flag to show the list of objects to the console as they are run.
* `--dryrun` - only list the inventory file contents, do not run the S3 API call.

## Behavior

This script will make an `s3 client head_object` call for each line in the CSV file.

If the object is in Glacier, there is a value in ['ResponseMetadata']['HTTPHeaders']['x-amz-restore']

This value is a single line that looks like `ongoing-request="false", expiry-date="Fri, 26 Feb 2021 00:00:00 GMT"`

* `ongoing-request` is set to "true" if the restore from Glacier is still happening

If an object is not there or inaccessible, an error is logged and the job continues.
## Examples

### Batch01 showing each object

This example assumes you have the `restore-check.py` file and inventory file in the current directory.
```
python restore-check.py --inventory_file inventory-test-100.csv --batchname batch01 --show
```
### Batch01 not showing each object to the display

This runs the inventory and doesn't show the inventory files as they are processed

```
python restore-check.py --inventory_file inventory-test-100.csv --batchname batch01
```

### Batch01 only checking the last 100 objects.

This checks the last 100 objects.

```
python restore-check.py --inventory_file inventory-test-100.csv --batchname batch01 --last 100
```

### Batch01 inventory in S3 only checking the last 100 objects.

This checks the last 100 objects based on an Inventory file stored in S3.

```
python restore-check.py --inventory_file s3://my_bucket/2021-02-10/inventory-test-all.csv --batchname batch01 --last 100
```

This checks the last 100 objects based on an Inventory file that is gzipped and stored in S3.

```
python restore-check.py --inventory_file s3://my_bucket/2021-02-11/inventory-test-all.csv.gz --batchname batch01 --last 100
```


## Todo
* Unit and Use-Case Testing

