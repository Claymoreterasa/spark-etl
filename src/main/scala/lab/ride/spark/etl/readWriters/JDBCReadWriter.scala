package lab.ride.spark.etl.readWriters

import lab.ride.spark.etl.utils.{PropertiesUtils, SparkUtils}
import org.apache.spark.sql.SaveMode

/**
  * JDBC读取写入，读取写入额外可配置项见, 不同数据库需要在依赖中添加不同驱动
  * https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader
  * https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter
  *
  * overwrite: overwrite the existing data.
  * append: append the data.
  * ignore: ignore the operation (i.e. no-op).
  * error or errorifexists: default option, throw an exception at runtime.
  *
  * 输入：JDBC连接配置，数据表名
  * 输出：JDBC连接配置，数据表名， 表格式（可选）
  */
object JDBCReadWriter {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getLocalSession()

    // 数据库配置,可以添加batchsize等等数据库支持的配置
    val properties = PropertiesUtils.getProperties("db.properties")
    val sqlProperties = new java.util.Properties()
    sqlProperties.setProperty("user", properties.getProps("mysql.user"))
    sqlProperties.setProperty("password", properties.getProps("mysql.password"))

    val url = properties.getProps("mysql.url")
    val tbName = "user"

    spark.read.jdbc(url, tbName, sqlProperties)
            .write
            .mode(SaveMode.Overwrite)
            .option("createTableColumnTypes", "password VARCHAR(1024), username CHAR(64), mobile CHAR(64)") //设置输出表结构
            .jdbc(url, tbName, sqlProperties)
    spark.stop()
  }
}
