package lab.ride.spark.etl.readWriters
import lab.ride.spark.etl.utils.{PropertiesUtils, SparkUtils}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf

object HBaseReadWriter {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getLocalSession()
    val sc = spark.sparkContext
    val conf = HBaseConfiguration.create()

    val properties = PropertiesUtils.getProperties("db.properties")
    val inputTableName = "student"
    val outputTableName = "student02"
    val clientPort = properties.getProps("hbase.zookeeper.property.clientPort")
    val quorum = properties.getProps("hbase.zookeeper.quorum")
    val master = properties.getProps("hbase.master")
    conf.set(TableInputFormat.INPUT_TABLE, inputTableName)
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, outputTableName)

    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum",quorum)
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", clientPort)
    conf.set("hbase.master", master)

    //读取数据并转化成rdd
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[org.apache.hadoop.hbase.mapred.TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, outputTableName)

    hBaseRDD.map{case (_,result) =>{
      //获取行键
      (Bytes.toString(result.getRow),
      //通过列族和列名获取列
      Bytes.toString(result.getValue("info".getBytes,"name".getBytes)),
      Bytes.toInt(result.getValue("info".getBytes,"age".getBytes)),
      Bytes.toInt(result.getValue("info".getBytes,"mobile".getBytes)))
    }}.map{t=>{
      val put = new Put(Bytes.toBytes(t._1))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(t._2))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(t._3))
      put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(t._4))
      (new ImmutableBytesWritable, put)
    }}.saveAsHadoopDataset(jobConf)
    sc.stop()
  }
}
