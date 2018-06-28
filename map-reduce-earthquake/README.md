# To put input file into hdfs
hdfs dfs -put input.csv

# To compile map reduce job
'''
javac -cp `hadoop classpath` *.java
'''

# To create jar file of map reduce job
jar cf EarthQuakeAnalysis.jar *.class

# To run EarthQuakeAnalysis.jar job
hadoop jar EarthQuakeAnalysis.jar EarthQuakeAnalysis /user/pradeep.s.rajpoot_gmail/input.csv /user/pradeep.s.rajpoot_gmail/earth1

