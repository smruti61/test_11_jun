# test_11_jun

Command to build docker:
docker build -t basf_test .

command to run the pyspark code:
 docker run -it basf_test spark-submit --master 'local[*]' pyspark_test.py
