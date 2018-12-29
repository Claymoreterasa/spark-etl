package lab.ride.spark.etl.readWriters

import lab.ride.spark.etl.utils.{PropertiesUtils, SparkUtils}
import org.apache.spark.sql.SaveMode

object HdfsReadWriter {
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtils.getProperties("db.properties")
    val filepath: String = properties.getProps("hdfs.url")
    val outPath: String = properties.getProps("hdfsOutPath")

    val spark = SparkUtils.getLocalSession()
    spark.read.textFile(filepath)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("permissions", "false")
      .text(outPath)
  }


}
