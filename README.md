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

### Pulsar 2.9.* Sink Setup

````


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
