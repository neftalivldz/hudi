%pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext, Row, Window
from pyspark.sql.functions import col, datediff, to_date
from pyspark.sql import functions as F
import datetime
spark = SparkSession \
    .builder \
    .appName("Hudi Python Spark basic example") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.hive.convertMetastoreParquet", "false") \
    .config("spark.sql.crossJoin.enabled","true") \
    .getOrCreate()

## Create a simple data set
data = [(1594836776,1,"HR","James ","Smith","36636","M",3000),
        (1594836776,2,"HR","Michael ","Rose","40288","M",4000),
        (1594836776,3,"Finance","Robert ","Williams","42114","M",4000),
        (1594836777,4,"Finance","Maria ","Jones","39192","F",4000),
        (1594836777,5,"Sales","Jen","Brown","","F",-1)]

columns = ["ts","id","department","firstname","lastname","dob","gender","salary"]

df = spark.createDataFrame(data, columns)


## Need a record key, cannot be null
## A partition, in this case department
## Precombine field, in this case ts (tie breaker for record key)
tableName = 'EjemploHudi'
basePath = "s3://xxx/output/Ejemplo_HUDI"

hudi_options_ex = {
  'hoodie.table.name': tableName_ex,
  'hoodie.datasource.write.recordkey.field': 'id',
  'hoodie.datasource.write.partitionpath.field': 'department',
  'hoodie.datasource.write.table.name': tableName_ex,
  'hoodie.datasource.write.operation': 'insert',
  'hoodie.datasource.write.precombine.field': 'ts',
  'hoodie.upsert.shuffle.parallelism': 2
}
# And save
df.write.format("hudi").options(**hudi_options_ex).mode("overwrite").save(basePath_ex)

## Read the table
## You can change the first /* for a specific partition
df = spark.read.format("hudi").load(basePath_ex+ "/*/*")
df.show()

## Create a new dataframe to add some rows
dataap = [(1594836796,6,"HR","Jaime ","Sanchez","3666","M",300),
       (1594836786,7,"HR","Miguel ","","4028","M",400),
       (1594836796,8,"Finance","Roberto ","Williams","4114","M",400),
       (1594836796,9,"Finance","Mary ","Jones","3912","F",400),
       (1594836796,10,"","Juan","Cafe","","F",-1)]

columns = ["ts", "id", "firstname","lastname","dob","gender","salary"]

dfap = spark.createDataFrame(dataap, columns)

## Append the rows to the original dataframe
dfap.write.format("hudi").options(**hudi_options_ex).mode("append").save(basePath_ex)

## Read the entire dataframe
df = spark.read.format("hudi").load(basePath_ex+ "/*/*")
df.show()

## Read incremental
## First identify the commits your want to read
commits = list(map(lambda row: row[0], spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime").limit(50).collect()))
beginTime = commits[len(commits) - 2] # commit time we are interested in

# incrementally query data
incremental_read_options = {
  'hoodie.datasource.query.type': 'incremental',
  'hoodie.datasource.read.begin.instanttime': beginTime,
}

## Read the file with the incremental options
df_Incremental = spark.read.format("hudi").options(**incremental_read_options).load(basePath_ex)
df_Incremental.show()

## Delete Finance records
df_delete = df.where(col('department') == 'Finance').select('id','department').whitColumn('ts', F.lit("0"))
df_delete.show()

# issue deletes
hudi_delete_options = {
  'hoodie.table.name': tableName,
  'hoodie.datasource.write.recordkey.field': 'id',
  'hoodie.datasource.write.partitionpath.field': 'department',
  'hoodie.datasource.write.table.name': tableName,
  'hoodie.datasource.write.operation': 'delete',
  'hoodie.datasource.write.precombine.field': 'ts',
  'hoodie.upsert.shuffle.parallelism': 2, 
  'hoodie.insert.shuffle.parallelism': 2
}

df.write.format("hudi").options(**hudi_delete_options).mode("append").save(basePath)


df = spark.read.format("hudi").load(basePath_ex+ "/*/*")
df.show()
df.where(col('department')=='Finance').show()

## Update it's justa a delete + append
## Read the readme.md to see how to configure Zeppelin in AWS EMR to work with Apache Hudi
