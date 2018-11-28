package com.hortonworks.segy

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.spark.{HBaseContext, KeyFamilyQualifier}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job

import scala.collection.mutable

object Spark2HBase2 {

  def main(args: Array[String]) {

    val rowKeyField = "id"
    val quorum = "per-dc-m01,per-dc-m02,per-dc-m03"
    val clientPort = "2181"
    val hBaseTempTable = "bulkloadtable"

    val sparkConf = new SparkConf().setAppName("Spark2HBase2")
    val sc = new SparkContext(sparkConf)

    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum", quorum)
    hBaseConf.set("hbase.zookeeper.property.clientPort", clientPort)

    creteHTable(hBaseTempTable, hBaseConf)

    val hbaseContext = new HBaseContext(sc, hBaseConf)

    val records_to_simulate = 100000
    val rdd = sc.parallelize(1 to records_to_simulate)

    val rddnew = rdd.map(x => {
      (Bytes.toBytes(x), Array((Bytes.toBytes("info"), "c1".getBytes(), x.toString.getBytes())))
    })

    println("[ *** ] Printing simulated data (first 10 records): ")
    rddnew.map(x => x._2.toString).take(10).foreach(x => println(x))

    hbaseContext.bulkLoad[Put](rddnew.map(record => {
      val put = new Put(record._1)
      record._2.foreach(putValue => put.addColumn(putValue._1, putValue._2, putValue._3))
      put
    }), TableName.valueOf(hBaseTempTable), (t : Put) => putForLoad(t), "/tmp/bulkload")

    val conn = ConnectionFactory.createConnection(hBaseConf)
    val hbTableName = TableName.valueOf(hBaseTempTable.getBytes())
    val regionLocator = new HRegionLocator(hbTableName, classOf[ClusterConnection].cast(conn))
    val realTable = conn.getTable(hbTableName)
    HFileOutputFormat2.configureIncrementalLoad(Job.getInstance(), realTable, regionLocator)

    // bulk load start
    val loader = new LoadIncrementalHFiles(hBaseConf)
    val admin = conn.getAdmin
    loader.doBulkLoad(new Path("/tmp/bulkload"),admin,realTable,regionLocator)

    sc.stop()
  }

  def creteHTable(tableName: String, hBaseConf : Configuration) = {
    val connection = ConnectionFactory.createConnection(hBaseConf)
    val hBaseTableName = TableName.valueOf(tableName)
    val admin = connection.getAdmin
    if (!admin.tableExists(hBaseTableName)) {
      val tableDesc = new HTableDescriptor(hBaseTableName)
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes))
      admin.createTable(tableDesc)
    }
    connection.close()
  }

  /**
    * Prepare the Put object for bulkload function.
    * @param put The put object.
    * @throws java.io.IOException
    * @throws java.lang.InterruptedException
    * @return Tuple of (KeyFamilyQualifier, bytes of cell value)*/
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def putForLoad(put: Put): Iterator[(KeyFamilyQualifier, Array[Byte])] = {
    val ret: mutable.MutableList[(KeyFamilyQualifier, Array[Byte])] = mutable.MutableList()
    import scala.collection.JavaConversions._
    for (cells <- put.getFamilyCellMap.entrySet().iterator()) {
      val family = cells.getKey
      for (value <- cells.getValue) {
        val kfq = new KeyFamilyQualifier(CellUtil.cloneRow(value), family, CellUtil.cloneQualifier(value))
        ret.+=((kfq, CellUtil.cloneValue(value)))
      }
    }
    ret.iterator
  }
}
