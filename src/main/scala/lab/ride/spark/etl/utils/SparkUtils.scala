package lab.ride.spark.etl.utils

import org.apache.spark.sql.SparkSession

/**
  * @author cwz
  * @date 2018/12/08
  * @since
  */
object SparkUtils {
  def getLocalSession(): SparkSession ={
     val spark = SparkSession
      .builder()
      .appName("Spark Local")
      .master("local")
      .config("spark.sql.orc.impl", "native")
      .getOrCreate()
    spark
  }
}
