java -version
hdfs dfs -rm -r /user/yb1059/project/profile2
yarn classpath
javac -classpath `yarn classpath` -d . ProfileMapper.java
javac -classpath `yarn classpath` -d . ProfileReducer.java
javac -classpath `yarn classpath`:. -d . Profile.java
jar -cvf Profile.jar *.class
hadoop jar Profile.jar Profile /user/yb1059/project/cleanTest/part-r-00000 /user/yb1059/project/profile2
hdfs dfs -ls /user/yb1059/project/profile2
hdfs dfs -cat /user/yb1059/project/profile2/part-r-00000
