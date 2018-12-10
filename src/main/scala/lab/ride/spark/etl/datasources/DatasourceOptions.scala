package lab.ride.spark.etl.datasources

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 数据源读写操作
  */
trait DatasourceOptions {
  /**
    * 从数据源读取数据到Dataset
    * @param session Spark入口
    * @param options 配置
    * @param format 输入格式
    */
  def read(session: SparkSession, options: Map[String, String], format: String): DataFrame

  /**
    * 把Dataset数据写到数据源
    * @param session Spark入口
    * @param dataFrame 写出数据
    * @param options 配置
    * @param format 输出格式
    */
  def write(session: SparkSession, dataFrame: DataFrame, options: Map[String, String], format: String)
}
