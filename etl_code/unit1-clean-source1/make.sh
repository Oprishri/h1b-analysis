java -version
yarn classpath
javac -classpath `yarn classpath` -d . CleanMapper.java
javac -classpath `yarn classpath` -d . CleanReducer.java
javac -classpath `yarn classpath`:. -d . CleanDriver.java
jar -cvf clean.jar *.class