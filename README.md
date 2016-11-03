# WordCount
Java word count program using Hadoop MapReduce

Note: This program requires Hadoop environment to run.

Steps to run the word count program:

1. Copy the "wordcount.jar" archive to your local drive
2. run the "wordcount.jar" with the below command
  - hadoop jar wordcount.jar cd.mapreduce.wordcount.main.WordCount <input file path> <output file path>
3. Once the MapReduce job is complete, run a cat on the created part file to view the output
  - hdfs dfs -cat <path of the part file>
4. Done!
