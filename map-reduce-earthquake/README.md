# Seismic Analytics:

Suppose you have just become the Development Lead for a company which specializes in reading seismic data which measure earthquake magnitudes around the world. There are thousands of such sensors deployed around the world recording earthquake data in log files, the following format:

Src,Eqid,Version,Datetime,Lat,Lon,Magnitude,Depth,NST, Region

Example:
nc,71920701,1,”Saturday, January 12, 2013 19:43:18 UTC”,38.7865,-122.7630,1.5,1.10,27,“Northern California”

Each entry consists of lot of details. The items in red are the magnitude of the earthquake and the name of region where the reading was taken, respectively.
Your Director of Software asks you to perform a simple task: for every region where sensors were deployed, find out the highest magnitude of the earthquake recorded.

# To put input file into hdfs
hdfs dfs -put input.csv

# To compile map reduce job
javac -cp `hadoop classpath` *.java

# To create jar file of map reduce job
jar cf EarthQuakeAnalysis.jar *.class

# To run EarthQuakeAnalysis.jar job
hadoop jar EarthQuakeAnalysis.jar EarthQuakeAnalysis /user/pradeep.s.rajpoot_gmail/input.csv /user/pradeep.s.rajpoot_gmail/earth1

