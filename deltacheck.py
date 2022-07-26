# Install pip3 install delta-spark==2.0.0
# Install pip3 install pyspark==3.2.0

from delta.tables import *
from pyspark.sql.functions import *
import pyspark
from delta import *

builder = pyspark.sql.SparkSession.builder.appName("PulsarApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.read.format("delta").load("/opt/demo/lakehouse/")
df.printSchema()
df.select("uuid","humidity","co2","cputempf","datetimestamp","ts").orderBy("datetimestamp").show(5,100)
