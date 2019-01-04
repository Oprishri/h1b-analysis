java -version
hdfs dfs -rm -r /user/yb1059/project/cleanOutputUpdate
yarn classpath
javac -classpath `yarn classpath` -d . DataCleanMapper.java 
javac -classpath `yarn classpath` -d . DataCleanReducer.java 
javac -classpath `yarn classpath`:. -d . DataClean.java 
jar -cvf DataClean.jar *.class 
hadoop jar DataClean.jar DataClean /user/yb1059/project/rawData.txt /user/yb1059/project/cleanOutputUpdate
hdfs dfs -ls /user/yb1059/project/cleanOutputUpdate
hdfs dfs -cat /user/yb1059/project/cleanOutputUpdate/part-r-00000 | head -2