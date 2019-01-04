# delete the old output folder
hdfs dfs -rm -r /user/my1843/project/wordCount
# run hadoop
hadoop jar wordCount.jar WordCountDriver /user/my1843/project/distinct_employer_name.tsv /user/my1843/project/wordCount