hadoop jar Vector.jar Vector /user/yb1059/project/ML/vector.txt  /user/yb1059/project/vectorFile
hdfs dfs -ls /user/yb1059/project/vectorFile
hdfs dfs -cat /user/yb1059/project/vectorFile/part-r-00000 | head -2

hdfs dfs -get /user/yb1059/project/vectorFile/part-r-00000
mv part-r-00000 VectorData