
yes test string | head -c 2GB > 2gb-file.log
aws s3 cp 2gb-file.log s3://jnicamzn-glacier-copy-test-root/Large-Files/
rm -R 2gb-file.log

yes test string | head -c 5GB > 5gb-file.log
aws s3 cp 5gb-file.log s3://jnicamzn-glacier-copy-test-root/Large-Files/
rm -R 5gb-file.log

yes test string | head -c 6GB > 6gb-file.log
aws s3 cp 6gb-file.log s3://jnicamzn-glacier-copy-test-root/Large-Files/
rm -R 6gb-file.log

yes test string | head -c 7GB > 7gb-file.log
aws s3 cp 7gb-file.log s3://jnicamzn-glacier-copy-test-root/Large-Files/
rm -R 7gb-file.log

yes test string | head -c 8GB > 8gb-file.log
aws s3 cp 8gb-file.log s3://jnicamzn-glacier-copy-test-root/Large-Files/
rm -R 8gb-file.log

yes test string | head -c 9GB > 9gb-file.log
aws s3 cp 9gb-file.log s3://jnicamzn-glacier-copy-test-root/Large-Files/
rm -R 9gb-file.log

yes test string | head -c 10GB > 10gb-file.log
aws s3 cp 10gb-file.log s3://jnicamzn-glacier-copy-test-root/Large-Files/
rm -R 10gb-file.log

yes test string | head -c 11GB > 11gb-file.log
aws s3 cp 11gb-file.log s3://jnicamzn-glacier-copy-test-root/Large-Files/
rm -R 11gb-file.log
