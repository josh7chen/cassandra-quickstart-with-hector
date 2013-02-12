
Cassandra quickstart with hector
===============================

Prerequisites for Building
-------------------------
Java JDK 1.6
Maven 2.2 or higher (http://maven.apache.org/)

Start the project 
-----------------
$ mvn archetype:create -DgroupId=com.lei.cassandra.sample  -DartifactId=cassandra-quickstart-with-hector   -DarchetypeArtifactId=maven-archetype-quickstart

Generate Eclipse Project File
-----------------------------
$ mvn eclipse:eclipse

Build
-------------------
$ mvn clean package


Test
-------------------
$ mvn test  



Run
-------------------


Setup Keyspace, Column Family, 
$ bin/cassandra-cli -host localhost -port 9160  < /path/to/script/demo_script.txt 
$ cassandra-cli -host localhost -port 9160  < ./demo_script.txt 

$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoTest -Dlog4j.configuration=file:./log4j.info.xml

$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoTest

# To fetch column values:
$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoGetColumnValues

# To query a column value for a key:
$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoQueryRowColumn

# To query columnis for a key:
$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoQueryRowColumnList


# To insert a row and its columns: 
$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoInsertColumnValues


# To delete a row column 
$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoDeleteColumnValues

# To delete a row 
$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoDeleteRow

# To list all column values 
$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoListAllColumnValues

# To query on a secondary index: 
$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoQuerySecondaryIndex

# To query on a secondary index with raw bytes: 
$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.DemoQuerySecondaryIndexRawBytes


$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.KeyspaceTest  -Dlog4j.configuration=file:./log4j.debug.xml 

$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.KeyspaceTest  -Dlog4j.configuration=file:./log4j.info.xml 

$ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.lei.cassandra.sample.KeyspaceTest


