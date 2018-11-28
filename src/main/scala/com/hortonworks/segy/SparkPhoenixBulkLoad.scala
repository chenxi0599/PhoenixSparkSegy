package com.hortonworks.segy

import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, TableMapReduceUtil}
import org.apache.hadoop.hbase.io.hfile.HFile

import org.apache.hadoop.mapreduce.Job

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap
import scala.io.Source.fromFile

import java.util.Calendar

object SparkPhoenixBulkLoad{

  def main(args: Array[String]) {

    val start_time = Calendar.getInstance()
    println("[ *** ] Start Time: " + start_time.getTime().toString)

    /***************************************************************
      *   Parameters
      ****************************************************************/
    val props = getProps(args(0))
    val records_to_simulate = props.getOrElse("records_to_simulate", "1000000").toInt
    val htablename          = props.getOrElse("htablename", "phoenixtable")
    val columnfamily        = props.getOrElse("columnfamily", "cf")
    val hfile_output        = props.getOrElse("hfile_output", "/tmp/phoenix_files")

    val sparkConf = new SparkConf().setAppName("SparkPhoenixHFiles")
    val sc = new SparkContext(sparkConf)

    println("[ *** ] Creating HBase Configuration")

    val hConf = HBaseConfiguration.create()
    hConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    hConf.set("hbase.zookeeper.quorum", "per-dc-m01,per-dc-m02,per-dc-m03")
    hConf.set("hbase.mapreduce.hfileoutputformat.table.name", htablename)
    hConf.set(TableInputFormat.INPUT_TABLE, htablename)

    val tableName: TableName = TableName.valueOf(htablename)

    /***************************************************************
      *   Simulate Data
      ****************************************************************/
    println("[ *** ] Simulating " + records_to_simulate.toString() + " records...")
    val rdd = sc.parallelize(1 to records_to_simulate)

    println("[ *** ] Creating KeyValues")
    val rdd_out = rdd.map(x => {
      val kv: KeyValue = new KeyValue( Bytes.toBytes(x), "cf".getBytes(), "c1".getBytes(), x.toString.getBytes() )
      (new ImmutableBytesWritable( Bytes.toBytes(x) ), kv)
    })

    println("[ *** ] Printing simulated data (first 10 records): ")
    rdd_out.map(x => x._2.toString).take(10).foreach(x => println(x))

    /***************************************************************
      *   Write to HFiles
      ****************************************************************/

    println("[ *** ] Saving HFiles to HDFS ("+hfile_output.toString()+")")
    rdd_out.saveAsNewAPIHadoopFile(
      hfile_output,
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[HFileOutputFormat2],
      hConf)

    println("[ *** ] Setting up HBase configuration")
    val job: Job = Job.getInstance(hConf, "phoenixbulkload")
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    TableMapReduceUtil.initCredentials(job)

    val conn = ConnectionFactory.createConnection(hConf)
    val htable = conn.getTable(tableName)
    val regionLocator = conn.getRegionLocator(tableName)
    println("[ *** ] Setting up HFile Incremental Load")
    HFileOutputFormat2.configureIncrementalLoad(job, htable, regionLocator)

    // Print Runtime
    val end_time = Calendar.getInstance()
    println("[ *** ] End Time: " + end_time.getTime().toString)
    println("[ *** ] Total Runtime: " + ((end_time.getTimeInMillis() - start_time.getTimeInMillis()).toFloat/1000).toString + " seconds")

    sc.stop()

  }


  def getProps(file: => String): HashMap[String,String] = {
    val props = new HashMap[String,String]
    val lines = fromFile(file).getLines
    lines.foreach(x => if (x contains "=") props.put(x.split("=")(0), if (x.split("=").size > 1) x.split("=")(1) else null))
    props
  }

}

