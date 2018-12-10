package lab.ride.spark.etl.utils

import lab.ride.spark.etl.datasources.DatasourceOptions
import org.apache.spark.sql._

/**
  * 数据源读取写入入口类
  * @author cwz
  * @date 2018/12/10
  * @since
  */
object DatasourceUtils{

  /**
    * 读取数据源数据到DataFrame
    * @param datasource
    * @param options
    * @param format
    * @return
    */
  def read(datasource: DatasourceOptions, options: Map[String, String], format: String): DataFrame = {
    val session = SparkUtils.getLocalSession()
    return datasource.read(session, options, format)
  }

  /**
    * 写出DataFrame数据到数据源
    * @param datasource
    * @param dataFrame
    * @param options
    * @param format
    */
  def write(datasource: DatasourceOptions, dataFrame: DataFrame, options: Map[String, String], format: String): Unit = {
    val session = SparkUtils.getLocalSession()
    datasource.write(session, dataFrame, options, format)
    session.stop()
  }
}
