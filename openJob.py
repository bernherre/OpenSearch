import sys
from awsglue.utils import getResolvedOptions
from awsglue.transforms import (...)
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.types import (...)
from pyspark.sql.functions import (...)

args=getResolvedOptions(sys.ardv, 
                        [ ...])

var1= arg["..."]

config_especials={"spark.sql.session.timeZone":"UTC",
                  "spark.sql...
}
conf=SparkConf()
if config_espacials and isinstance(config_especials,dict):
    for k,v in config_especials.items():
      conf.set(k,v)
      
sc= SparkContext(conf=conf)
glueContext=GlueContext(sc)
spark=glueContext.spark_session
job=Job(Context)
job.init(args['JOB_NAME',args])

df=(glueContext.read.forma('parquet')
    .option("partitionOverwriteMode", "dynamic")
    .load("MyS3Path")
)

df_final=(
df.withColumn("key",concat(df.col1,df.col2,...))
  .withColumn("alpha", struct(
    col("col1").alias("col1Name"),
    col("col2").alias("col2Name"),
    struct(
      col("col3").alias("col3Name"),
      col("col4").alias("col4Name"),).alias("subStruct2"),
    ),
      col("col5").cast(LongType()).alias("col5Name"),
      ...),
.withColumn("SuperStructure2", split(col("arrayCol"),",").cast(array_type_string))
...,)
df_final.printSchema()

(
  df_final.write.format("org.elasticsearch.spark.sql")
  .option("es.nodes",host)
  .option("es.port",port)
  .option("es.nodes.wan.only","true")
  .option("es.nodes.discovery","false")
  .option("es.resource",f"{index_name}")##Index name in openSearch--not Alias never
  .option("es.indes.auto.create","true") ##Can be false for custom mapping
  .option("es.mapping.id","key")""document key
  .option("es.alias",f"{alias_name}") ##only in case that alias exists and index be mapping to alias
  .option("es.net.ssl","true")
  .option("es.net.http.auth.user",username)
  .option("es.net.http.auth.password",password) ## could be write other options like batchsize or time, etc...
  .save()
)
job.commit()
print("JobEnds")
