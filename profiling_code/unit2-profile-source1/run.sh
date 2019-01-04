# delete the old output folder
hdfs dfs -rm -r /user/my1843/project/profile
# run hadoop
hadoop jar profile.jar ProfileDriver /user/my1843/project/data20.tsv /user/my1843/project/profile