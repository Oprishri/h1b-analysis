java -version
yarn classpath
javac -classpath `yarn classpath` -d . CategoryMapper.java
javac -classpath `yarn classpath` -d . CategoryReducer.java
javac -classpath `yarn classpath`:. -d . CategoryDriver.java
jar -cvf category.jar *.class