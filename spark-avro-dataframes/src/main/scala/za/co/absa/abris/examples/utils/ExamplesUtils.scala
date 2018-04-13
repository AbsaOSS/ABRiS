package za.co.absa.abris.examples.utils

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.streaming.DataStreamReader
import za.co.absa.abris.avro.read.confluent.SchemaManager

import scala.collection.JavaConverters._

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

  implicit class SchemaRegistryConfiguration(props: Properties) {
    def getSchemaRegistryConfigurations(subscribeParamKey: String): Map[String,String] = {
      val keys = Set(SchemaManager.PARAM_SCHEMA_REGISTRY_URL, SchemaManager.PARAM_SCHEMA_ID)
      val confs = scala.collection.mutable.Map[String,String](SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> props.getProperty(subscribeParamKey))
      for (propKey <- keys) yield {
        if (props.containsKey(propKey)) {
          confs += propKey -> props.getProperty(propKey)
        }
      }
      Map[String,String](confs.toSeq:_*)
    }
  }
}