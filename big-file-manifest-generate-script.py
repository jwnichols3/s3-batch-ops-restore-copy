# generate a temporary mainfiest file for x number of yGB files
# Change the following variables to customize:
#   total_files = how many files
#   size_of_file = #GB
#   script_file = custom script name
#   target_bucket_prefix = S3 URI of the bucket + prefix

loop_count = 1
total_files = 100
size_of_file = "3GB"
script_file = "big-file-manifest-temp.csv"
target_bucket = "jnicamzn-glacier-copy-test-root"  # CHANGE_ME
target_prefix = "Large-Files/"  # CHANGE_ME

script_f = open(script_file, "w")

while loop_count <= total_files:
    filename = target_prefix + size_of_file.lower() + "-file-" + \
        str(loop_count) + ".log"

    output = "\"" + target_bucket + "\",\"" + filename + "\"\n"

    print(output)
    script_f.write(output)
    loop_count += 1
