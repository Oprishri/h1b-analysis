# delete the old output folder
hdfs dfs -rm -r /user/my1843/project/clean
# run hadoop
hadoop jar clean.jar CleanDriver /user/my1843/project/h1b.txt /user/my1843/project/clean