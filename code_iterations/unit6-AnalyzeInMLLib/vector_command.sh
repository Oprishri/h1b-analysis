#dumbo
#"code" below is a text file saving the NetID password
beeline -u jdbc:hive2://babar.es.its.nyu.edu:10000/ -n yb1059 -w code --outputformat=tsv2 -e "use yb1059; select * from vector" >> vector.tsv
mv vector.tsv vector.txt
hdfs dfs -rm vector.txt /user/yb1059/project/ML/vector.txt
hdfs dfs -put vector.txt /user/yb1059/project/ML
#then go to local work station to do ML analysis
