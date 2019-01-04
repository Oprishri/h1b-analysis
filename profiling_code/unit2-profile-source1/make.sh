java -version
yarn classpath
javac -classpath `yarn classpath` -d . ProfileMapper.java
javac -classpath `yarn classpath` -d . ProfileReducer.java
javac -classpath `yarn classpath`:. -d . ProfileDriver.java
jar -cvf profile.jar *.class