# AWS S3 Batch Operations Large File Copy Batch

Github Repo: https://github.com/jwnichols3/s3-batch-ops-restore-status-check

Why do you need this? 

The S3 Batch Operations Copy jobs use the S3 PUT Object API to process object copies. Th S3 PUT Object API has a 5GB limit.

This script uses the Boto3 S3 Client Copy method which automatically deals with large files, multi-part copies, backoff, and retries.

There are two logfiles produced:
* `copy-batch-BATCHNAME-YYYY-MM-DD-EPOCH-detail.log`
* `copy-batch-BATCHNAME-YYYY-MM-DD-EPOCH-summary.log`
  
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
* `--target_bucket` - the target bucket for the objects.
* `--dryrun` - only list the inventory file contents, do not run the S3 API call.

## Behavior

This script will make an `s3 client copy` call for each line in the CSV file.

If an object is not there or inaccessible, an error is logged and the job continues.

## Examples

### Batch01 showing each object

This example assumes you have the `copy-batch.py` file and inventory file in the current directory.

```
python copy-batch.py --inventory_file inventory-test-100.csv --batchname batch01 --target_bucket s3_bucket_target
```

### Batch01 with the inventory file in S3 in GZIP format

This example reads the inventory from S3 from a file in GZIP format.

```
python copy-batch.py --inventory_file s3://bucket_name/inventory-test-100.csv.gz --batchname batch01 --target_bucket s3_bucket_target
```


## Generating Large Files (Linux) for Testing

Ideal method is to use an EC2 instance to generate these so you're able to use the AWS network. Here are a set of commands to use to generate multi-GB sized files. Note: you will need an instance that has more memory in GB than the largest file you want to create. Also, your volume has to support the size of files as well (alternatively you can create, copy, then delete the file sequencially).

```
dd if=/dev/zero of=2gb-file bs=1000000000 count=2
dd if=/dev/zero of=3gb-file bs=1000000000 count=3
dd if=/dev/zero of=4gb-file bs=1000000000 count=4
dd if=/dev/zero of=5gb-file bs=1000000000 count=5
dd if=/dev/zero of=6gb-file bs=1000000000 count=6
dd if=/dev/zero of=7gb-file bs=1000000000 count=7
dd if=/dev/zero of=8gb-file bs=1000000000 count=8
dd if=/dev/zero of=9gb-file bs=1000000000 count=9
dd if=/dev/zero of=10gb-file bs=1000000000 count=10
dd if=/dev/zero of=11gb-file bs=1000000000 count=11

```

Copy the files to the source S3-bucket for testing

```
s3 cp ./2g-file s3://target_bucket
```



## Todo
* Unit and Use-Case Testing

