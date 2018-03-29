package za.co.absa.abris.examples.utils

import java.util.Properties
import java.io.FileInputStream
import org.apache.spark.sql.streaming.DataStreamReader
import scala.collection.JavaConverters._
import org.apache.spark.sql.DataFrameWriter

object ExamplesUtils {

  private val OPTION_PREFIX = "option."

  def loadProperties(path: String): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream(path))
    properties
  }

  private def getKeys(properties: Properties) = {
    properties.keySet().asScala
      .filter(key => key.toString().startsWith(OPTION_PREFIX))      
      .map(key => (key.toString, key.toString.drop(OPTION_PREFIX.length())))
  }

  implicit class ReaderStreamOptions(stream: DataStreamReader) {
    def addOptions(properties: Properties): DataStreamReader = {
      getKeys(properties)
        .foreach(keys => {          
          println(s"DataStreamReader: setting option: ${keys._2} = ${properties.getProperty(keys._1)}")
          stream.option(keys._2, properties.getProperty(keys._1))
        })
      stream
    }
  }

  implicit class WriterStreamOptions(stream: DataFrameWriter[Array[Byte]]) {
    def addOptions(properties: Properties): DataFrameWriter[Array[Byte]] = {
      getKeys(properties)
        .foreach(keys => {          
          println(s"DataStreamReader: setting option: ${keys._2} = ${properties.getProperty(keys._1)}")
          stream.option(keys._2, properties.getProperty(keys._1))
        })
      stream
    }
  }
}