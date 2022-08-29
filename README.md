# FLiP-Pi-DeltaLake-Thermal

Apache Pulsar -> Sink -> DeltaLake 

![Diagram](https://raw.githubusercontent.com/tspannhw/FLiP-Pi-DeltaLake-Thermal/main/thermaldelta.png)

### Python Pulsar Record

*** Note:   current version requires no nulls, no maps. ***

````
class thermal(Record):
    uuid = String(required=True)
    ipaddress = String(required=True)
    cputempf = Integer(required=True)
    runtime = Integer(required=True)
    host = String(required=True)
    hostname = String(required=True)
    macaddress = String(required=True)
    endtime = String(required=True)
    te = String(required=True)
    cpu = Float(required=True)
    diskusage = String(required=True)
    memory = Float(required=True)
    rowid = String(required=True)
    systemtime = String(required=True)
    ts = Integer(required=True)
    starttime = String(required=True)
    datetimestamp = String(required=True)
    temperature = Float(required=True)
    humidity = Float(required=True)
    co2 =  Float(required=True)

````

### Raspberry Pi Sensor Python Run

````
2022-07-15 09:08:06.115 INFO  [3034530880] HandlerBase:64 | [persistent://public/default/pi-sensors-partition-0, ] Getting connection from pool
2022-07-15 09:08:06.117 INFO  [3034530880] ClientConnection:182 | [<none> -> pulsar://pulsar1:6650] Create ClientConnection, timeout=10000
2022-07-15 09:08:06.117 INFO  [3034530880] ConnectionPool:96 | Created connection for pulsar://127.0.0.1:6650
2022-07-15 09:08:06.120 INFO  [3034530880] ClientConnection:370 | [192.168.1.204:47626 -> 192.168.1.230:6650] Connected to broker through proxy. Logical broker: pulsar://127.0.0.1:6650
2022-07-15 09:08:06.127 INFO  [3034530880] ProducerImpl:189 | [persistent://public/default/pi-sensors-partition-0, ] Created producer on broker [192.168.1.204:47626 -> 192.168.1.230:6650]
SCD4X, Serial: d3efd3efd3ef
{'_required_default': False, '_default': None, '_required': False, 'uuid': 'thrml_fui_20220715130806', 'ipaddress': '192.168.1.204', 'cputempf': 106, 'runtime': 0, 'host': 'thermal', 'hostname': 'thermal', 'macaddress': 'e4:5f:01:7c:3f:34', 'endtime': '1657890486.6318262', 'te': '0.0005743503570556641', 'cpu': 0.0, 'diskusage': '105078.3 MB', 'memory': 9.0, 'rowid': '20220715130806_60188a3a-57f0-4bc3-819b-5b643e0de5b9', 'systemtime': '07/15/2022 09:08:12', 'ts': 1657890492, 'starttime': '07/15/2022 09:08:06', 'datetimestamp': '2022-07-15 13:08:11.308836+00:00', 'temperature': 27.6959, 'humidity': 31.7, 'co2': 1360.0}
{'_required_default': False, '_default': None, '_required': False, 'uuid': 'thrml_wcc_20220715130812', 'ipaddress': '192.168.1.204', 'cputempf': 106, 'runtime': 0, 'host': 'thermal', 'hostname': 'thermal', 'macaddress': 'e4:5f:01:7c:3f:34', 'endtime': '1657890492.320293', 'te': '0.0007376670837402344', 'cpu': 0.0, 'diskusage': '105078.3 MB', 'memory': 9.0, 'rowid': '20220715130812_c59a07d1-8994-49e8-888a-44f82f7ed441', 'systemtime': '07/15/2022 09:08:17', 'ts': 1657890497, 'starttime': '07/15/2022 09:08:12', 'datetimestamp': '2022-07-15 13:08:16.084772+00:00', 'temperature': 27.2527, 'humidity': 32.3, 'co2': 1365.0}
{'_required_default': False, '_default': None, '_required': False, 'uuid': 'thrml_wvx_20220715130817', 'ipaddress': '192.168.1.204', 'cputempf': 106, 'runtime': 0, 'host': 'thermal', 'hostname': 'thermal', 'macaddress': 'e4:5f:01:7c:3f:34', 'endtime': '1657890497.0999746', 'te': '0.0007336139678955078', 'cpu': 3.3, 'diskusage': '105078.3 MB', 'memory': 9.0, 'rowid': '20220715130817_af0783b4-12f4-49f3-b5e2-d4ad912d369b', 'systemtime': '07/15/2022 09:08:21', 'ts': 1657890501, 'starttime': '07/15/2022 09:08:17', 'datetimestamp': '2022-07-15 13:08:20.862417+00:00', 'temperature': 26.983, 'humidity': 32.78, 'co2': 1381.0}
{'_required_default': False, '_default': None, '_required': False, 'uuid': 'thrml_cgk_20220715130821', 'ipaddress': '192.168.1.204', 'cputempf': 105, 'runtime': 0, 'host': 'thermal', 'hostname': 'thermal', 'macaddress': 'e4:5f:01:7c:3f:34', 'endtime': '1657890501.8731027', 'te': '0.0007915496826171875', 'cpu': 0.0, 'diskusage': '105078.3 MB', 'memory': 9.0, 'rowid': '20220715130821_bb918b7c-f074-4cd0-8315-ab7d773cd9d2', 'systemtime': '07/15/2022 09:08:26', 'ts': 1657890506, 'starttime': '07/15/2022 09:08:21', 'datetimestamp': '2022-07-15 13:08:25.636997+00:00', 'temperature': 26.7052, 'humidity': 33.3, 'co2': 1349.0}
^C2022-07-15 09:08:30.165 INFO  [3069380992] ClientImpl:495 | Closing Pulsar client with 1 producers and 0 consumers
2022-07-15 09:08:30.166 INFO  [3069380992] ProducerImpl:686 | [persistent://public/default/pi-sensors-partition-0, standalone-1-22] Closing producer for topic persistent://public/default/pi-sensors-partition-0
2022-07-15 09:08:30.169 INFO  [3034530880] ProducerImpl:729 | [persistent://public/default/pi-sensors-partition-0, standalone-1-22] Closed producer
2022-07-15 09:08:30.170 INFO  [3034530880] ClientConnection:1548 | [192.168.1.204:47626 -> 192.168.1.230:6650] Connection closed
2022-07-15 09:08:30.171 INFO  [3034530880] ClientConnection:1548 | [192.168.1.204:47624 -> 192.168.1.230:6650] Connection closed

````

### Example Message

````

{
 "uuid": "thrml_zda_20220715182748",
 "ipaddress": "192.168.1.204",
 "cputempf": 108,
 "runtime": 0,
 "host": "thermal",
 "hostname": "thermal",
 "macaddress": "e4:5f:01:7c:3f:34",
 "endtime": "1657909668.7279365",
 "te": "0.0007398128509521484",
 "cpu": 1.8,
 "diskusage": "105078.0 MB",
 "memory": 9.0,
 "rowid": "20220715182748_fc4cbbb1-79da-4c1a-8991-78bd23c9f221",
 "systemtime": "07/15/2022 14:27:53",
 "ts": 1657909673,
 "starttime": "07/15/2022 14:27:48",
 "datetimestamp": "2022-07-15 18:27:52.492469+00:00",
 "temperature": 28.238,
 "humidity": 29.61,
 "co2": 992.0
}

````


### Pulsar Delta Sink Configuration

* conf/deltalakesink.yml

````


configs:
        type: "delta"
        maxCommitInterval: 120
        maxRecordsPerCommit: 10_000_000
        tablePath: "file:///opt/demo/lakehouse"
        processingGuarantees: "EXACTLY_ONCE"
        deltaFileType: "parquet"
        subscriptionType: "Failover"
````

### Pulsar 2.9.* Sink Setup

````

bin/pulsar-admin topics delete "persistent://public/default/pi-sensors" --force
bin/pulsar-admin schemas delete "persistent://public/default/pi-sensors"
bin/pulsar-admin topics unload "persistent://public/default/pi-sensors"

bin/pulsar-admin sink stop --name delta_sink --namespace default --tenant public
bin/pulsar-admin sinks delete --tenant public --namespace default --name delta_sink

bin/pulsar-client consume "persistent://public/default/pi-sensors" -s "pisensorwatch" --subscription-type Failover

        
bin/pulsar-admin sinks create --archive ./connectors/pulsar-io-lakehouse-2.9.2.24.nar --tenant public --namespace default --name delta_sink --sink-config-file conf/deltalakesink.yml --inputs "persistent://public/default/pi-sensors" --parallelism 1  --subs-name pisensorwatch --processing-guarantees EFFECTIVELY_ONCE


bin/pulsar-admin sinks get --tenant public --namespace default --name delta_sink

{
  "tenant": "public",
  "namespace": "default",
  "name": "delta_sink",
  "className": "org.apache.pulsar.ecosystem.io.lakehouse.SinkConnector",
  "sourceSubscriptionName": "pisensorwatch",
  "sourceSubscriptionPosition": "Latest",
  "inputs": [
    "persistent://public/default/pi-sensors"
  ],
  "inputSpecs": {
    "persistent://public/default/pi-sensors": {
      "isRegexPattern": false,
      "schemaProperties": {},
      "consumerProperties": {},
      "poolMessages": false
    }
  },
  "configs": {
    "maxRecordsPerCommit": 10000000,
    "processingGuarantees": "EXACTLY_ONCE",
    "tablePath": "file:///opt/demo/lakehouse",
    "subscriptionType": "Failover",
    "deltaFileType": "parquet",
    "type": "delta",
    "maxCommitInterval": 120
  },
  "parallelism": 1,
  "processingGuarantees": "EFFECTIVELY_ONCE",
  "retainOrdering": true,
  "autoAck": true
}

bin/pulsar-admin sinks status --tenant public --namespace default --name delta_sink
{
  "numInstances" : 1,
  "numRunning" : 1,
  "instances" : [ {
    "instanceId" : 0,
    "status" : {
      "running" : true,
      "error" : "",
      "numRestarts" : 0,
      "numReadFromPulsar" : 66,
      "numSystemExceptions" : 0,
      "latestSystemExceptions" : [ ],
      "numSinkExceptions" : 0,
      "latestSinkExceptions" : [ ],
      "numWrittenToSink" : 66,
      "lastReceivedTime" : 1657890506653,
      "workerId" : "c-standalone-fw-127.0.0.1-8080"
    }
  } ]
}

bin/pulsar-admin sinks list

[
  "scylla-airquality-sink",
  "delta_sink"
]


````

### Output

````

/opt/demo/lakehouse/
total 12
-rw-r--r-- 1 root root    0 Jul 15 09:00 part-0000-f1bfd140-8da9-4507-8e8d-1d98c5faf67a-c000.snappy.parquet
drwxr-xr-x 2 root root 4096 Jul 15 09:00 _delta_log
-rw-r--r-- 1 root root 6968 Jul 15 09:00 part-0000-de100d45-4aae-444f-b078-85fd54662a8f-c000.snappy.parquet

cat /opt/demo/lakehouse/_delta_log/00000000000000000001.json
{"commitInfo":{"timestamp":1657890016312,"operation":"WRITE","operationParameters":{},"readVersion":0,"isolationLevel":"Serializable","isBlindAppend":true,"operationMetrics":{},"engineInfo":"pulsar-sink-connector-version-2.9.1 Delta-Standalone/0.3.0"}}
{"txn":{"appId":"pulsar-delta-sink-connector","version":1,"lastUpdated":1657890016310}}
{"add":{"path":"part-0000-de100d45-4aae-444f-b078-85fd54662a8f-c000.snappy.parquet","partitionValues":{},"size":4460,"modificationTime":1657890016310,"dataChange":true,"stats":"{}"}}

````

### Querying Delta Lake (Scala Spark Shell)

````

bin/spark-shell --packages io.delta:delta-core_2.12:1.2.1 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

import io.delta.tables._
import org.apache.spark.sql.functions._

val df = spark.read.format("delta").load("/opt/demo/lakehouse/")
df.printSchema()

df.select("uuid","humidity","co2","cputempf","datetimestamp","ts").orderBy(col("datetimestamp").desc).show(5,100)

````

### Querying Delta Lake pyspark

````

pyspark --packages io.delta:delta-core_2.12:1.2.1 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

>>> df = spark.read.format("delta").load("/opt/demo/lakehouse/")
>>> df.show(10)
+--------------------+-------------+--------+-------+-------+--------+-----------------+------------------+--------------------+---+-----------+------+--------------------+-------------------+----------+-------------------+--------------------+-----------+--------+------+
|                uuid|    ipaddress|cputempf|runtime|   host|hostname|       macaddress|           endtime|                  te|cpu|  diskusage|memory|               rowid|         systemtime|        ts|          starttime|       datetimestamp|temperature|humidity|   co2|
+--------------------+-------------+--------+-------+-------+--------+-----------------+------------------+--------------------+---+-----------+------+--------------------+-------------------+----------+-------------------+--------------------+-----------+--------+------+
|thrml_aoi_2022071...|192.168.1.204|     106|      0|thermal| thermal|e4:5f:01:7c:3f:34|1657890015.3147886|0.000545024871826...|0.0|105078.3 MB|   9.0|20220715130015_dc...|07/15/2022 09:00:20|1657890020|07/15/2022 09:00:15|2022-07-15 13:00:...|    24.7132|    37.3|1332.0|
|thrml_crz_2022071...|192.168.1.204|     106|      0|thermal| thermal|e4:5f:01:7c:3f:34|1657890020.0944705|0.000758171081542...|0.0|105078.3 MB|   9.0|20220715130020_b1...|07/15/2022 09:00:24|1657890024|07/15/2022 09:00:20|2022-07-15 13:00:...|    24.6491|   37.46|1334.0|
|thrml_nre_2022071...|192.168.1.204|     106|      0|thermal| thermal|e4:5f:01:7c:3f:34|1657890024.8735719| 0.00072479248046875|0.0|105078.3 MB|   9.0|20220715130024_48...|07/15/2022 09:00:29|1657890029|07/15/2022 09:00:24|2022-07-15 13:00:...|    24.5797|   37.66|1336.0|
|thrml_umx_2022071...|192.168.1.204|     106|      0|thermal| thermal|e4:5f:01:7c:3f:34|1657890029.6493442|0.000723361968994...|0.0|105078.3 MB|   9.0|20220715130029_3d...|07/15/2022 09:00:34|1657890034|07/15/2022 09:00:29|2022-07-15 13:00:...|    24.4462|   37.83|1339.0|
|thrml_ijc_2022071...|192.168.1.204|     106|      0|thermal| thermal|e4:5f:01:7c:3f:34|1657890034.5295844|0.000723361968994...|6.5|105078.3 MB|   9.0|20220715130034_f9...|07/15/2022 09:00:39|1657890039|07/15/2022 09:00:34|2022-07-15 13:00:...|    24.3901|   38.07|1343.0|
|thrml_yfy_2022071...|192.168.1.204|     107|      0|thermal| thermal|e4:5f:01:7c:3f:34|1657890039.3061981|  0.0005035400390625|0.0|105078.3 MB|   9.0|20220715130039_f6...|07/15/2022 09:00:44|1657890044|07/15/2022 09:00:39|2022-07-15 13:00:...|      24.31|   38.23|1343.0|
|thrml_jzo_2022071...|192.168.1.204|     106|      0|thermal| thermal|e4:5f:01:7c:3f:34|1657890044.0833697|0.000773191452026...|0.0|105078.3 MB|   9.0|20220715130044_78...|07/15/2022 09:00:48|1657890048|07/15/2022 09:00:44|2022-07-15 13:00:...|    24.2486|   38.31|1345.0|
|thrml_onk_2022071...|192.168.1.204|     105|      0|thermal| thermal|e4:5f:01:7c:3f:34|  1657890048.86295|0.000727176666259...|4.9|105078.3 MB|   9.0|20220715130048_5b...|07/15/2022 09:00:53|1657890053|07/15/2022 09:00:48|2022-07-15 13:00:...|    24.1578|   38.41|1346.0|
|thrml_lrx_2022071...|192.168.1.204|     106|      0|thermal| thermal|e4:5f:01:7c:3f:34|1657890053.6392615|0.000728368759155...|0.0|105078.3 MB|   9.0|20220715130053_35...|07/15/2022 09:00:58|1657890058|07/15/2022 09:00:53|2022-07-15 13:00:...|    24.1311|   38.49|1347.0|
|thrml_wqr_2022071...|192.168.1.204|     106|      0|thermal| thermal|e4:5f:01:7c:3f:34|1657890058.4192047|0.000722885131835...|0.0|105078.3 MB|   9.0|20220715130058_4e...|07/15/2022 09:01:03|1657890063|07/15/2022 09:00:58|2022-07-15 13:01:...|    24.0536|   38.62|1348.0|
+--------------------+-------------+--------+-------+-------+--------+-----------------+------------------+--------------------+---+-----------+------+--------------------+-------------------+----------+-------------------+--------------------+-----------+--------+------+
only showing top 10 rows

df.printSchema()
root
 |-- uuid: string (nullable = false)
 |-- ipaddress: string (nullable = false)
 |-- cputempf: integer (nullable = false)
 |-- runtime: integer (nullable = false)
 |-- host: string (nullable = false)
 |-- hostname: string (nullable = false)
 |-- macaddress: string (nullable = false)
 |-- endtime: string (nullable = false)
 |-- te: string (nullable = false)
 |-- cpu: float (nullable = false)
 |-- diskusage: string (nullable = false)
 |-- memory: float (nullable = false)
 |-- rowid: string (nullable = false)
 |-- systemtime: string (nullable = false)
 |-- ts: integer (nullable = false)
 |-- starttime: string (nullable = false)
 |-- datetimestamp: string (nullable = false)
 |-- temperature: float (nullable = false)
 |-- humidity: float (nullable = false)
 |-- co2: float (nullable = false)
 
 
df.select("humidity","co2","datetimestamp","cputempf","ts", "uuid").show(100)

df.select("uuid","humidity","co2","cputempf","datetimestamp","ts").show(5,100)

df.select("uuid","humidity","co2","cputempf","datetimestamp","ts").orderBy("datetimestamp").show(5,100)

````

### Python3 Setup

````
pip3 install -U pip
pip3 install delta-spark==2.0.0
pip3 install pyspark==3.2.0
````

### Example Python3 App

````
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


````

### Validating Parquet Files


````

pip3 install parquet-tools -U

parquet-tools inspect /opt/demo/lakehouse/part-0000-f2185319-1e50-4ca9-a8dd-69b7e54e552e-c000.snappy.parquet

############ file meta data ############
created_by: parquet-mr version 1.12.0 (build db75a6815f2ba1d1ee89d1a90aeb296f1f3a8f20)
num_columns: 20
num_rows: 3
num_row_groups: 1
format_version: 1.0
serialized_size: 3289


############ Columns ############
uuid
ipaddress
cputempf
runtime
host
hostname
macaddress
endtime
te
cpu
diskusage
memory
rowid
systemtime
ts
starttime
datetimestamp
temperature
humidity
co2

############ Column(uuid) ############
name: uuid
path: uuid
max_definition_level: 0
max_repetition_level: 0
physical_type: BYTE_ARRAY
logical_type: String
converted_type (legacy): UTF8

############ Column(ipaddress) ############
name: ipaddress
path: ipaddress
max_definition_level: 0
max_repetition_level: 0
physical_type: BYTE_ARRAY
logical_type: String
converted_type (legacy): UTF8

############ Column(cputempf) ############
name: cputempf
path: cputempf
max_definition_level: 0
max_repetition_level: 0
physical_type: INT32
logical_type: None
converted_type (legacy): NONE

############ Column(runtime) ############
name: runtime
path: runtime
max_definition_level: 0
max_repetition_level: 0
physical_type: INT32
logical_type: None
converted_type (legacy): NONE

############ Column(host) ############
name: host
path: host
max_definition_level: 0
max_repetition_level: 0
physical_type: BYTE_ARRAY
logical_type: String
converted_type (legacy): UTF8

############ Column(hostname) ############
name: hostname
path: hostname
max_definition_level: 0
max_repetition_level: 0
physical_type: BYTE_ARRAY
logical_type: String
converted_type (legacy): UTF8

############ Column(macaddress) ############
name: macaddress
path: macaddress
max_definition_level: 0
max_repetition_level: 0
physical_type: BYTE_ARRAY
logical_type: String
converted_type (legacy): UTF8

############ Column(endtime) ############
name: endtime
path: endtime
max_definition_level: 0
max_repetition_level: 0
physical_type: BYTE_ARRAY
logical_type: String
converted_type (legacy): UTF8

############ Column(te) ############
name: te
path: te
max_definition_level: 0
max_repetition_level: 0
physical_type: BYTE_ARRAY
logical_type: String
converted_type (legacy): UTF8

############ Column(cpu) ############
name: cpu
path: cpu
max_definition_level: 0
max_repetition_level: 0
physical_type: FLOAT
logical_type: None
converted_type (legacy): NONE

############ Column(diskusage) ############
name: diskusage
path: diskusage
max_definition_level: 0
max_repetition_level: 0
physical_type: BYTE_ARRAY
logical_type: String
converted_type (legacy): UTF8

############ Column(memory) ############
name: memory
path: memory
max_definition_level: 0
max_repetition_level: 0
physical_type: FLOAT
logical_type: None
converted_type (legacy): NONE

############ Column(rowid) ############
name: rowid
path: rowid
max_definition_level: 0
max_repetition_level: 0
physical_type: BYTE_ARRAY
logical_type: String
converted_type (legacy): UTF8

############ Column(systemtime) ############
name: systemtime
path: systemtime
max_definition_level: 0
max_repetition_level: 0
physical_type: BYTE_ARRAY
logical_type: String
converted_type (legacy): UTF8

############ Column(ts) ############
name: ts
path: ts
max_definition_level: 0
max_repetition_level: 0
physical_type: INT32
logical_type: None
converted_type (legacy): NONE

############ Column(starttime) ############
name: starttime
path: starttime
max_definition_level: 0
max_repetition_level: 0
physical_type: BYTE_ARRAY
logical_type: String
converted_type (legacy): UTF8

############ Column(datetimestamp) ############
name: datetimestamp
path: datetimestamp
max_definition_level: 0
max_repetition_level: 0
physical_type: BYTE_ARRAY
logical_type: String
converted_type (legacy): UTF8

############ Column(temperature) ############
name: temperature
path: temperature
max_definition_level: 0
max_repetition_level: 0
physical_type: FLOAT
logical_type: None
converted_type (legacy): NONE

############ Column(humidity) ############
name: humidity
path: humidity
max_definition_level: 0
max_repetition_level: 0
physical_type: FLOAT
logical_type: None
converted_type (legacy): NONE

############ Column(co2) ############
name: co2
path: co2
max_definition_level: 0
max_repetition_level: 0
physical_type: FLOAT
logical_type: None
converted_type (legacy): NONE
````

### Flink SQL

````

CREATE CATALOG pulsar WITH (
   'type' = 'pulsar',
   'service-url' = 'pulsar://pulsar1:6650',
   'admin-url' = 'http://pulsar1:8080',
   'format' = 'json'
);

use catalog pulsar;

show tables;

describe `pi-sensors`

select uuid, cputempf, temperature, humidity, co2, ts, datetimestamp from `pi-sensors`;

select max(cputempf) as MaxCPU, max(temperature) as MaxTemp, max(humidity) as MaxHumidity, max(co2) as MaxCO2, datetimestamp 
from `pi-sensors` 
group by datetimestamp;

````

![FlinkSQLTableDef](https://raw.githubusercontent.com/tspannhw/FLiP-Pi-DeltaLake-Thermal/main/flinkthermal.png)

![FlinkSQLResults](https://raw.githubusercontent.com/tspannhw/FLiP-Pi-DeltaLake-Thermal/main/flinksqlresultsthermal.png)

![FlinkSQLCluster](https://raw.githubusercontent.com/tspannhw/FLiP-Pi-DeltaLake-Thermal/main/flinkthermaldisplay.png)

### Pulsar SQL 

````

presto> desc pulsar."public/default"."pi-sensors";
      Column       |   Type    | Extra |                                   Comment
-------------------+-----------+-------+-----------------------------------------------------------------------------
 uuid              | varchar   |       | "string"
 ipaddress         | varchar   |       | "string"
 cputempf          | integer   |       | "int"
 runtime           | integer   |       | "int"
 host              | varchar   |       | "string"
 hostname          | varchar   |       | "string"
 macaddress        | varchar   |       | "string"
 endtime           | varchar   |       | "string"
 te                | varchar   |       | "string"
 cpu               | real      |       | "float"
 diskusage         | varchar   |       | "string"
 memory            | real      |       | "float"
 rowid             | varchar   |       | "string"
 systemtime        | varchar   |       | "string"
 ts                | integer   |       | "int"
 starttime         | varchar   |       | "string"
 datetimestamp     | varchar   |       | "string"
 temperature       | real      |       | "float"
 humidity          | real      |       | "float"
 co2               | real      |       | "float"
 __partition__     | integer   |       | The partition number which the message belongs to
 __event_time__    | timestamp |       | Application defined timestamp in milliseconds of when the event occurred
 __publish_time__  | timestamp |       | The timestamp in milliseconds of when event as published
 __message_id__    | varchar   |       | The message ID of the message used to generate this row
 __sequence_id__   | bigint    |       | The sequence ID of the message used to generate this row
 __producer_name__ | varchar   |       | The name of the producer that publish the message used to generate this row
 __key__           | varchar   |       | The partition key for the topic
 __properties__    | varchar   |       | User defined properties
(28 rows)

presto> select co2, humidity,temperature, datetimestamp, ts, uuid, __publish_time__ from pulsar."public/default"."pi-sensors";
  co2   | humidity | temperature |          datetimestamp           |     ts     |           uuid           |    __publish_time__
--------+----------+-------------+----------------------------------+------------+--------------------------+-------------------------
 1042.0 |    32.55 |     26.4275 | 2022-07-15 18:44:34.803190+00:00 | 1657910675 | thrml_ska_20220715184430 | 2022-07-15 14:44:35.807
 1045.0 |    32.44 |     26.4676 | 2022-07-15 18:44:39.582550+00:00 | 1657910680 | thrml_udc_20220715184435 | 2022-07-15 14:44:40.587
 1046.0 |    32.49 |     26.4596 | 2022-07-15 18:44:44.358047+00:00 | 1657910685 | thrml_sqj_20220715184440 | 2022-07-15 14:44:45.362
 1047.0 |    32.48 |     26.4729 | 2022-07-15 18:44:49.133060+00:00 | 1657910690 | thrml_kff_20220715184445 | 2022-07-15 14:44:50.137
 1047.0 |    32.49 |     26.4649 | 2022-07-15 18:44:53.912035+00:00 | 1657910694 | thrml_xll_20220715184450 | 2022-07-15 14:44:54.916
 1048.0 |    32.43 |     26.4783 | 2022-07-15 18:44:58.790554+00:00 | 1657910699 | thrml_zkr_20220715184454 | 2022-07-15 14:44:59.793
 1049.0 |     32.5 |     26.4569 | 2022-07-15 18:45:03.567302+00:00 | 1657910704 | thrml_ogp_20220715184459 | 2022-07-15 14:45:04.571
 1050.0 |    32.53 |     26.4409 | 2022-07-15 18:45:08.347039+00:00 | 1657910709 | thrml_xto_20220715184504 | 2022-07-15 14:45:09.351
 1050.0 |    32.51 |     26.4409 | 2022-07-15 18:45:13.118426+00:00 | 1657910714 | thrml_kpz_20220715184509 | 2022-07-15 14:45:14.123
 1051.0 |    32.48 |     26.4382 | 2022-07-15 18:45:17.897249+00:00 | 1657910718 | thrml_ghy_20220715184514 | 2022-07-15 14:45:18.902
 1052.0 |     32.5 |     26.4676 | 2022-07-15 18:45:22.676866+00:00 | 1657910723 | thrml_uei_20220715184518 | 2022-07-15 14:45:23.680
 1052.0 |    32.41 |     26.4676 | 2022-07-15 18:45:27.552872+00:00 | 1657910728 | thrml_mzi_20220715184523 | 2022-07-15 14:45:28.557
 1051.0 |    32.36 |     26.4569 | 2022-07-15 18:45:32.331788+00:00 | 1657910733 | thrml_luk_20220715184528 | 2022-07-15 14:45:33.336
 1052.0 |    32.33 |     26.4783 | 2022-07-15 18:45:37.104204+00:00 | 1657910738 | thrml_tbp_20220715184533 | 2022-07-15 14:45:38.107
 1051.0 |    32.33 |     26.5317 | 2022-07-15 18:45:41.881281+00:00 | 1657910742 | thrml_dqf_20220715184538 | 2022-07-15 14:45:42.887
 1050.0 |    32.43 |     26.5557 | 2022-07-15 18:45:46.659983+00:00 | 1657910747 | thrml_nal_20220715184542 | 2022-07-15 14:45:47.665
 1051.0 |    32.38 |     26.5824 | 2022-07-15 18:45:51.537531+00:00 | 1657910752 | thrml_bxo_20220715184547 | 2022-07-15 14:45:52.543
 1050.0 |    32.39 |      26.553 | 2022-07-15 18:45:56.316338+00:00 | 1657910757 | thrml_gtv_20220715184552 | 2022-07-15 14:45:57.322
 1050.0 |     32.4 |     26.5824 | 2022-07-15 18:46:01.095270+00:00 | 1657910762 | thrml_keo_20220715184557 | 2022-07-15 14:46:02.099
 1050.0 |    32.31 |     26.5664 | 2022-07-15 18:46:05.865263+00:00 | 1657910766 | thrml_jmz_20220715184602 | 2022-07-15 14:46:06.869
 1050.0 |     32.4 |      26.529 | 2022-07-15 18:46:10.643571+00:00 | 1657910771 | thrml_hfg_20220715184606 | 2022-07-15 14:46:11.648
 1048.0 |    32.33 |     26.5317 | 2022-07-15 18:46:15.416817+00:00 | 1657910776 | thrml_xtg_20220715184611 | 2022-07-15 14:46:16.420
 1048.0 |    32.29 |     26.5557 | 2022-07-15 18:46:20.295350+00:00 | 1657910781 | thrml_vzo_20220715184616 | 2022-07-15 14:46:21.304
 1048.0 |    32.33 |     26.5637 | 2022-07-15 18:46:25.074648+00:00 | 1657910786 | thrml_mds_20220715184621 | 2022-07-15 14:46:26.084
 1047.0 |    32.36 |     26.5611 | 2022-07-15 18:46:29.851385+00:00 | 1657910790 | thrml_qgv_20220715184626 | 2022-07-15 14:46:30.856
 1049.0 |    32.41 |     26.5691 | 2022-07-15 18:46:34.629965+00:00 | 1657910795 | thrml_taj_20220715184630 | 2022-07-15 14:46:35.639
 1049.0 |    32.38 |     26.5797 | 2022-07-15 18:46:39.409163+00:00 | 1657910800 | thrml_zdj_20220715184635 | 2022-07-15 14:46:40.413
 1049.0 |    32.24 |     26.6225 | 2022-07-15 18:46:44.281726+00:00 | 1657910805 | thrml_pri_20220715184640 | 2022-07-15 14:46:45.290
 1048.0 |    32.18 |     26.5958 | 2022-07-15 18:46:49.060992+00:00 | 1657910810 | thrml_mhu_20220715184645 | 2022-07-15 14:46:50.066
 
````

![PulsarSQL](https://raw.githubusercontent.com/tspannhw/FLiP-Pi-DeltaLake-Thermal/main/pulsarsql.png)

### Apache Spark Structured Streaming SQL

````
val dfPulsar = spark.readStream.format("pulsar").option("service.url", "pulsar://pulsar1:6650").option("admin.url", "http://pulsar1:8080").option("topic", "persistent://public/default/pi-sensors").load()

dfPulsar.printSchema()

root
 |-- uuid: string (nullable = false)
 |-- ipaddress: string (nullable = false)
 |-- cputempf: integer (nullable = false)
 |-- runtime: integer (nullable = false)
 |-- host: string (nullable = false)
 |-- hostname: string (nullable = false)
 |-- macaddress: string (nullable = false)
 |-- endtime: string (nullable = false)
 |-- te: string (nullable = false)
 |-- cpu: float (nullable = false)
 |-- diskusage: string (nullable = false)
 |-- memory: float (nullable = false)
 |-- rowid: string (nullable = false)
 |-- systemtime: string (nullable = false)
 |-- ts: integer (nullable = false)
 |-- starttime: string (nullable = false)
 |-- datetimestamp: string (nullable = false)
 |-- temperature: float (nullable = false)
 |-- humidity: float (nullable = false)
 |-- co2: float (nullable = false)
 |-- __key: binary (nullable = true)
 |-- __topic: string (nullable = true)
 |-- __messageId: binary (nullable = true)
 |-- __publishTime: timestamp (nullable = true)
 |-- __eventTime: timestamp (nullable = true)
 |-- __messageProperties: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)

val pQuery = dfPulsar.selectExpr("*").writeStream.format("console").option("truncate", false).start()

````

### HTML Table Display

* To Run:   python3 -m http.server 8000
* See thermal.html

![ThermalHTML](https://raw.githubusercontent.com/tspannhw/FLiP-Pi-DeltaLake-Thermal/main/thermalhtml.png)

### References

* https://streamnative.io/blog/release/2022-08-17-announcing-the-delta-lake-sink-connector-for-apache-pulsar/
* https://github.com/streamnative/pulsar-io-lakehouse/blob/master/docs/lakehouse-sink.md
* https://github.com/streamnative/pulsar-io-lakehouse/blob/master/docs/delta-lake-demo.md
* https://github.com/tspannhw/FLiP-Pi-Thermal
* https://dzone.com/articles/pulsar-in-python-on-pi
* https://docs.delta.io/latest/quick-start.html#set-up-project
