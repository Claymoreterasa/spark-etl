package lab.ride.spark.etl.datasources

import lab.ride.spark.etl.utils.{DatasourceUtils, PropertiesUtils}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * JDBC类型数据源
  */
class JDBCDatasource(url: String, tbName: String, user: String, password: String) extends DatasourceOptions{

  override def read(session: SparkSession, options: Map[String, String], format: String): DataFrame = {
    val sqlProperties = new java.util.Properties()
    sqlProperties.setProperty("user", user)
    sqlProperties.setProperty("password", password)

    session.read.jdbc(url, tbName, sqlProperties)
  }

  override def write(session: SparkSession, dataFrame: DataFrame, options: Map[String, String], format: String): Unit = {
    val sqlProperties = new java.util.Properties()
    sqlProperties.setProperty("user", user)
    sqlProperties.setProperty("password", password)

    dataFrame.foreach(println(_))
    dataFrame
      .write
      .mode(SaveMode.valueOf(options.get("saveMode").getOrElse("Overwrite")))
      .option("createTableColumnTypes", format) //设置输出表结构
      .jdbc(url, tbName, sqlProperties)

    session.stop()
  }
}

/**
  * JDBC测试
  */
object JDBCDatasource{
  def main(args: Array[String]): Unit = {
    val properties = PropertiesUtils.getProperties("db.properties")

    val url = properties.getProps("mysql.url")
    val user = properties.getProps("mysql.user")
    val password = properties.getProps("mysql.password")


    val jdbcInput = new JDBCDatasource(url,"user_input", user, password)
    val jdbcOutput = new JDBCDatasource(url,"user_output", user, password)

    val dataFrame = DatasourceUtils.read(jdbcInput, Map.empty, null)
    DatasourceUtils.write(jdbcOutput, dataFrame, Map.empty, "password VARCHAR(1024), username CHAR(64), mobile CHAR(64)")
  }
}