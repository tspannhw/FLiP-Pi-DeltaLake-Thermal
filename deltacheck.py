from delta.tables import *
from pyspark.sql.functions import *

df = spark.read.format("delta").load("/opt/demo/lakehouse/")
df.printSchema()
df.select("uuid","humidity","co2","cputempf","datetimestamp","ts").orderBy("datetimestamp").show(5,100)
