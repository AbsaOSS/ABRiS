/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.abris.examples.utils

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.streaming.{DataStreamReader, DataStreamWriter}
import org.apache.spark.sql.{DataFrameWriter, Row, SparkSession}
import org.slf4j.LoggerFactory
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.examples.deprecated.KafkaAvroReader.{PARAM_JOB_MASTER, PARAM_JOB_NAME, PARAM_LOG_LEVEL}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

object ExamplesUtils {

  private val OPTION_PREFIX = "option."

  def checkArgs(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("No properties file specified.")
      System.exit(1)
    }
  }

  def getSparkSession(properties: Properties, jobNameProp: String, jobMasterProp: String, logLevelProp: String): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(properties.getProperty(jobNameProp))
      .master(properties.getProperty(jobMasterProp))
      .getOrCreate()

    spark.sparkContext.setLogLevel(properties.getProperty(logLevelProp))
    spark
  }

  def loadProperties(args: Array[String]): Properties = {
    println("Loading properties from: " + args(0))
    val properties = ExamplesUtils.loadProperties(args(0))
    for (key <- properties.keysIterator) {
      println(s"\t$key = ${properties.getProperty(key)}")
    }
    properties
  }

  def loadProperties(path: String): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream(path))
    properties
  }

  private def getKeys(properties: Properties) = {
    properties.keySet().asScala
      .filter(key => key.toString.startsWith(OPTION_PREFIX))
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

  implicit class WriterRowOptions(stream: DataFrameWriter[Row]) {
    def addOptions(properties: Properties): DataFrameWriter[Row] = {
      getKeys(properties)
        .foreach(keys => {
          println(s"DataFrameWriter: setting option: ${keys._2} = ${properties.getProperty(keys._1)}")
          stream.option(keys._2, properties.getProperty(keys._1))
        })
      stream
    }
  }

  implicit class WriterOptions(stream: DataFrameWriter[Array[Byte]]) {
    def addOptions(properties: Properties): DataFrameWriter[Array[Byte]] = {
      getKeys(properties)
        .foreach(keys => {
          println(s"DataFrameWriter: setting option: ${keys._2} = ${properties.getProperty(keys._1)}")
          stream.option(keys._2, properties.getProperty(keys._1))
        })
      stream
    }
  }

  implicit class WriterRowStreamOptions(stream: DataStreamWriter[Row]) {
    def addOptions(properties: Properties): DataStreamWriter[Row] = {
      getKeys(properties)
        .foreach(keys => {
          println(s"DataStreamWriter: setting option: ${keys._2} = ${properties.getProperty(keys._1)}")
          stream.option(keys._2, properties.getProperty(keys._1))
        })
      stream
    }
  }

  implicit class WriterStreamOptions(stream: DataStreamWriter[Array[Byte]]) {
    def addOptions(properties: Properties): DataStreamWriter[Array[Byte]] = {
      getKeys(properties)
        .foreach(keys => {
          println(s"DataStreamWriter: setting option: ${keys._2} = ${properties.getProperty(keys._1)}")
          stream.option(keys._2, properties.getProperty(keys._1))
        })
      stream
    }
  }

  implicit class SchemaRegistryConfiguration(props: Properties) {

    def getSchemaRegistryConfigurations(subscribeParamKey: String): Map[String,String] = {

      val keys = Set(
        SchemaManager.PARAM_SCHEMA_REGISTRY_URL,
        SchemaManager.PARAM_KEY_SCHEMA_ID,
        SchemaManager.PARAM_VALUE_SCHEMA_ID,
        SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY,
        SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY,
        SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY,
        SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY)

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
