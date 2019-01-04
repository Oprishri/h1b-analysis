# delete the old output folder
hdfs dfs -rm -r /user/my1843/project/add-one-column
# run hadoop
hadoop jar category.jar CategoryDriver /user/my1843/project/data20.tsv /user/my1843/project/add-one-column