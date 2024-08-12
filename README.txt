Sukhum Boondecharak
S3940976

Instructions:

1. Go to the local directory where all the files are
2. From your local directory, enter these commands:
	scp mycount.jar jumphost:~/
	scp <test_file> jumphost:~/

3. After all files are copied to jumphost, enter the following command to enter jumphost:
	ssh jumphost
4. From jumphost, enter these commands: 
	scp mycount.jar hadoop:~/
	scp <test_file> hadoop:~/

The above instruction are with the expectation that you used to enter jumphost and hadoop before, as well as having created an EMR master node in hadoop. If it is not the case, from your jumphost, enter the following command and wait about 15 minutes:
	./create_cluster.sh

5. After all files are copied to hadoop, enter the following command to enter hadoop:
	ssh hadoop
6. Enter the following command to see if all flies are already in hadoop:
	ls
7. From hadoop, enter the following command: 
	hadoop fs -mkdir /input
8. To activate Spark program, we recommend you to open another window and enter the following command:
	spark-submit --class streaming.SparkStreamingApp --master yarn --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j-spark.properties" --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j-spark.properties" --deploy-mode client mycount.jar hdfs:///input/ hdfs:///output/

9. Go back to the previous hadoop window, then enter the following command:
	hadoop fs -put <test_file> /input

10. To see the result for each test (example is the test 1), enter the following command:
	hadoop fs -cat /output/task*1/p*

11. To see the result for each task (example is the task A), enter the following command:
	hadoop fs -cat /output/taskA*/p*

12. Otherwise, you can enter the following command to see all directories, then navigate to your preferred result:
	hadoop fs -ls /output



===== Note =====

If you want to re-test the program, clear the results and inputs by entering these commands first:

	hadoop fs -rm -r /output
	hadoop fs -rm -r /input/*