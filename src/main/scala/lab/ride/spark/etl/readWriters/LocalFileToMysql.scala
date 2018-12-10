package lab.ride.spark.etl.readWriters

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author cwz
  * @date 2018/12/08
  * @since
  * @see https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameReader
  * @see https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrameWriter
  */
object LocalFileToMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("spark.sql.orc.impl", "native")
      .getOrCreate()

    // 与输入顺序一致
    val inputSchema = StructType(
      List(
        StructField("username", StringType, true),
        StructField("password", StringType, true),
        StructField("mobile", StringType, true)
      )
    )

    // 数据库用户名密码
    val sqlProperties = new java.util.Properties()
    sqlProperties.setProperty("user", "cwz")
    sqlProperties.setProperty("password", "123456")

    // json 写入mysql
//    spark.read.json("data/input/user.json")
//      .write.mode(SaveMode.Overwrite)
//      .option("createTableColumnTypes", "username CHAR(64), password VARCHAR(1024), mobile CHAR(64)")
//      .jdbc("jdbc:mysql://localhost:3306/spring_security?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "user", sqlProperties)
//
//    // csv 写入mysql

    // createTableColumnTypes 需要与schema名称一致
    spark.read.option("header", "true")
      .schema(inputSchema)    // 设置表结构
      .csv("data/input/user.csv")
      .write.mode(SaveMode.Overwrite)
      .option("createTableColumnTypes", "password VARCHAR(1024), username CHAR(64), mobile CHAR(64)")
      .jdbc("jdbc:mysql://localhost:3306/spring_security?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "user_input", sqlProperties)
//
//    // parquet 写入mysql
//    spark.read
//      .parquet("data/input/user.parquet")
//      .write.mode(SaveMode.Overwrite)
//      .jdbc("jdbc:mysql://localhost:3306/spring_security?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "user", sqlProperties)
//
//
//    // orc 写入mysql
//    spark.read
//      .orc("data/input/user.orc")
//      .write.mode(SaveMode.Overwrite)
//      .jdbc("jdbc:mysql://localhost:3306/spring_security?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "user", sqlProperties)


//    spark.read.jdbc("jdbc:mysql://localhost:3306/spring_security?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "user", sqlProperties)
//      .write.json("data/output/user.json")

//    spark.read.jdbc("jdbc:mysql://localhost:3306/spring_security?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "user", sqlProperties)
//      .write
//      .option("header", "true")
//      .csv("data/friends/")

//    spark.read.jdbc("jdbc:mysql://localhost:3306/spring_security?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "user", sqlProperties)
//      .write.parquet("data/output/user.parquet")
//
//    spark.read.jdbc("jdbc:mysql://localhost:3306/spring_security?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC", "user", sqlProperties)
//      .write.orc("data/output/user.orc")
  }

}
