# generate a temporary script file that will create x number of yGB files
# Change the following variables to customize:
#   total_files = how many files
#   size_of_file = #GB
#   script_file = custom script name
#   target_bucket_prefix = S3 URI of the bucket + prefix

loop_count = 1
total_files = 100
size_of_file = "3GB"
script_file = "big-file-generate-temp.sh"
target_bucket_prefix = "s3://bucket_name/prefix/"  # CHANGEME

script_f = open(script_file, "w")

while loop_count <= total_files:
    filename = size_of_file.lower() + "-file-" + str(loop_count) + ".log"
    output = "yes test string loop " + str(loop_count) + " | head -c " + \
        size_of_file + " > " + filename + "\n" + \
        "aws s3 cp " + filename + " " + target_bucket_prefix + "\n" +\
        "rm -R " + filename + "\n"

    print(output)
    script_f.write(output)
    loop_count += 1
