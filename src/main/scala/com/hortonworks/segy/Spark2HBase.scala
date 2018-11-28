package com.hortonworks.segy

import java.util.Date

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.util.ToolRunner
import org.apache.spark.{SparkConf, SparkContext}

object Spark2HBase {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Spark2HBase")

    val sc = new SparkContext()

    val columnFamily1 = "f1"

    val tableName = "spark2hbase"

    val conf = HBaseConfiguration.create()
    conf.set("hbase.client.pause", "3000")
    conf.set("hbase.client.retries.number", "5")
    conf.set("hbase.client.operation.timeout", "60000")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    conf.set("hbase.zookeeper.quorum", "per-dc-m01,per-dc-m02,per-dc-m03")
    conf.set("hbase.mapreduce.hfileoutputformat.table.name", tableName)

    val date = new Date().getTime

    val sourceRDD = sc.parallelize(Array(
      (Bytes.toBytes("40"), //40 is rowkey
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo1"))), //family, column, value
      (Bytes.toBytes("40"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("b"), Bytes.toBytes("foo2.b"))),
      (Bytes.toBytes("41"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("bar.1"))),
      (Bytes.toBytes("41"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("d"), Bytes.toBytes("bar.2"))),
      (Bytes.toBytes("50"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo2.a"))),
      (Bytes.toBytes("50"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("c"), Bytes.toBytes("foo2.c"))),
      (Bytes.toBytes("51"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo3"))),
      (Bytes.toBytes("52"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("a"), Bytes.toBytes("foo.1"))),
      (Bytes.toBytes("52"),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("b"), Bytes.toBytes("foo.2")))))

    val rdd = sourceRDD.map(x => {
      val rowKey = x._1
      val family = x._2._1
      val colum = x._2._2
      val value = x._2._3
      (new ImmutableBytesWritable(rowKey), new KeyValue(rowKey, family, colum, date, value))
    })

    val stagingFolder = "/tmp/stagingfolder1"
    rdd.saveAsNewAPIHadoopFile(stagingFolder,
      classOf[ImmutableBytesWritable],
      classOf[KeyValue],
      classOf[HFileOutputFormat2],
      conf)

    //val conn = ConnectionFactory.createConnection(conf)
    //val table = conn.getTable(TableName.valueOf(tableName))

    //try {
    //  val regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))
    //  val list= List( regionLocator.getAllRegionLocations)
    //  list.foreach(println)
    //  val job = Job.getInstance(conf)

    //  job.setJobName("DumpFile")
    //  job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    //  job.setMapOutputValueClass(classOf[KeyValue])
    //  HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

    //  val load = new LoadIncrementalHFiles(conf)
    //  load.doBulkLoad(new Path(stagingFolder), conn.getAdmin, table, regionLocator)

    //} finally {
    //  table.close()
    //  conn.close()
    //}
  }
}

