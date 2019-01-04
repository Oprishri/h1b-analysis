#run mapreduce: Vector 
hdfs dfs -rm -r /user/yb1059/project/vectorFile
yarn classpath
javac -classpath `yarn classpath` -d . VectorMapper.java 
javac -classpath `yarn classpath` -d . VectorReducer.java 
javac -classpath `yarn classpath`:. -d . Vector.java 
jar -cvf Vector.jar *.class 