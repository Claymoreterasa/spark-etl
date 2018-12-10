package lab.ride.spark.etl.readWriters

import lab.ride.spark.etl.utils.SparkUtils
import org.apache.spark.sql.SaveMode


/**
  * Orc 文件读取写入，读取写入额外可配置项见
  * https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader
  * https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter
  * 输入：文件地址，可配置项
  * 输出：文件地址，可配置项
  */
object OrcReadWriter {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getLocalSession()
    spark
      .read.orc("data/input/user.orc")
      .write
      .mode(SaveMode.Overwrite)
      .option("compression", "none")
      .orc("data/output/user.orc")

    spark.stop()
  }
}
