package lab.ride.spark.etl.datasources
import lab.ride.spark.etl.utils.{DatasourceUtils, PropertiesUtils}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.mapred.JobConf

import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class HBaseDatasource (clientPort: String, quorum: String, master: String, tbName: String) extends DatasourceOptions{
  override def read(session: SparkSession, options: Map[String, String], format: String): DataFrame = {
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum",quorum)
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", clientPort)
    conf.set("hbase.master", master)
    conf.set(TableInputFormat.INPUT_TABLE, tbName)
    val sc = session.sparkContext
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
      //转化为rdd[Row]
    .map(r=>{
      val rowBuffer = scala.collection.mutable.ArrayBuffer.empty[String]
      rowBuffer.append(Bytes.toString(r._2.getRow))
      options.foreach(t=>{
        rowBuffer.append(Bytes.toString(r._2.getValue(Bytes.toBytes(t._1.split(",")(0)),Bytes.toBytes(t._1.split(",")(1)))))
      })
      Row.fromSeq(rowBuffer)
    })
    val schemaBuffer = scala.collection.mutable.ListBuffer.empty[StructField]
    options.foreach(t=>{
     schemaBuffer.append(StructField(t._1.split(":")(1),StringType,true))
    }
    )
    val schema = StructType(schemaBuffer)
    //将rdd[Row]转化为DF
    session.createDataFrame(hbaseRDD,schema)
  }

  override def write(session: SparkSession, dataFrame: DataFrame, options: Map[String, String], format: String): Unit = {
    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum",quorum)
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", clientPort)
    val sc = session.sparkContext

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[org.apache.hadoop.hbase.mapred.TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tbName)

    dataFrame.foreach(println(_))
    dataFrame.rdd.map((row: Row) => {
      val  put = new Put(Bytes.toBytes(row.getString(0)))
      val rowBuffer = scala.collection.mutable.ArrayBuffer.empty[String]
      var i=1
      options.foreach(t=>{
        put.add(Bytes.toBytes(t._1.split(":")(0)), Bytes.toBytes(t._1.split(":")(1)), Bytes.toBytes(row.getString(i)))
        i=i+1
      })
      (new ImmutableBytesWritable(), put)
    }).saveAsHadoopDataset(jobConf)
  }
}

/**
  * hbase测试
  */
object HBaseDatasource{
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtils.getProperties("db.properties")

    val clientPort = properties.getProps("hbase.zookeeper.property.clientPort")
    val quorum = properties.getProps("hbase.zookeeper.quorum")
    val master = properties.getProps("hbase.master")

    val hBaseInput = new HBaseDatasource(clientPort,quorum, master,"student")
    val hBaseOutput = new HBaseDatasource(clientPort,quorum, master,"student02")
    val hBaseMap = Map("info:name" -> "String", "info:password" -> "String","info:mobile" -> "String")
    val dataFrame = DatasourceUtils.read(hBaseInput, hBaseMap, null)
    DatasourceUtils.write(hBaseOutput, dataFrame, hBaseMap, null)
  }
}