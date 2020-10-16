# hudi
Provides easy examples on how to use Apache Hudi with AWS EMR and Zeppelin


On command line run this:

hdfs dfs -mkdir -p /apps/hudi/lib

hdfs dfs -copyFromLocal /usr/lib/hudi/hudi-spark-bundle.jar /apps/hudi/lib/hudi-spark-bundle.jar

hdfs dfs -copyFromLocal /usr/lib/spark/external/lib/spark-avro.jar /apps/hudi/lib/spark-avro.jar

hdfs dfs -copyFromLocal /usr/lib/spark/external/lib/spark-avro_2.11-2.4.5-amzn-0.jar /apps/hudi/lib/spark-avro_2.11-2.4.5-amzn-0.jar


In Zeppelin - Interpreter - Spark add this options

spark.jars = hdfs:///apps/hudi/lib/hudi-spark-bundle.jar,hdfs:///apps/hudi/lib/spark-avro.jar

spark.serializer = org.apache.spark.serializer.KryoSerializer

spark.sql.hive.convertMetastoreParquet = false
