package lab.ride.spark.etl.utils

import java.util.Properties

/**
  * 读取configPath位置的*.properties文件
  * @param configPath
  */
class PropertiesUtils private(configPath: String){
  val props = new Properties()

  props.load(getClass.getClassLoader().getResourceAsStream(configPath))

  def getProps(key: String): String ={
     props.getProperty(key)
  }
}

object PropertiesUtils{
  def getProperties(configPath: String) = new PropertiesUtils(configPath)
}
