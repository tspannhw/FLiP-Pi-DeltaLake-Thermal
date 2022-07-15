# FLiP-Pi-DeltaLake-Thermal

Apache Pulsar -> Sink -> DeltaLake 

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

bin/pulsar-admin sinks status --tenant public --namespace default --name delta_sink

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

### References

* https://github.com/streamnative/pulsar-io-lakehouse/blob/master/docs/lakehouse-sink.md
* https://github.com/streamnative/pulsar-io-lakehouse/blob/master/docs/delta-lake-demo.md
* https://github.com/tspannhw/FLiP-Py-Pi-GasThermal
