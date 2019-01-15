package lab.ride.spark.etl.readWriters

import lab.ride.spark.etl.utils.SparkUtils
import org.apache.spark.sql.SaveMode

object TxtReadWriter {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.getLocalSession()
    spark
      .read
      .text("data/input/user.txt")
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .option("compression", "none")
      .text("data/output/user.txt")

    spark.stop()
  }

}
