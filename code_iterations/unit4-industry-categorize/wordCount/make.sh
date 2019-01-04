java -version
yarn classpath
javac -classpath `yarn classpath` -d . WordCountMapper.java
javac -classpath `yarn classpath` -d . WordCountReducer.java
javac -classpath `yarn classpath`:. -d . WordCountDriver.java
jar -cvf wordCount.jar *.class