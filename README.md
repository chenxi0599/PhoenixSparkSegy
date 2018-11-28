# PhoenixSparkSegy

Env:
* HDP3
* Spark 2.3.1
* HBase 2.0

## Compile and Run

```bash
sbt assembly
```

```bash
spark-submit --master local[4] --verbose --class com.hortonworks.segy.Spark2HBase2 target/scala-2.11/Yang.jar
spark-submit --master local[4] --verbose --class com.hortonworks.segy.Spark2HBase target/scala-2.11/Yang.jar
spark-submit --master local[4] --verbose --class com.hortonworks.segy.SparkPhoenixBulkLoad target/scala-2.11/Yang.jar config
```




