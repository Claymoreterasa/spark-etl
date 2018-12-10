package lab.ride.spark.etl.readWriters

import lab.ride.spark.etl.utils.SparkUtils
import org.apache.spark.sql.SaveMode

/**
  * CSV 文件读取写入，读取写入额外可配置项见
  * https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader
  * https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter
  * 输入：文件地址，可配置项
  * 输出：文件地址，可配置项
  */
object CsvReadWriter {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getLocalSession()
    spark
      .read
      .option("header", true)
      .option("sep", ",")
      .option("encoding", "UTF-8")
      .option("nullValue", "")
      .csv("data/input/user.csv")
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .option("compression", "none")
      .option("dateFormat","yyyy-MM-dd")
      .csv("data/output/user.csv")

    spark.stop()
  }
}
