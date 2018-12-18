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

package za.co.absa.abris.examples

import java.util.Properties

import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY
import za.co.absa.abris.examples.utils.ExamplesUtils

import scala.collection.JavaConversions._

object ConfluentKafkaAvroReader {

  private val PARAM_JOB_NAME = "job.name"
  private val PARAM_JOB_MASTER = "job.master"
  private val PARAM_KEY_AVRO_SCHEMA = "key.avro.schema"
  private val PARAM_PAYLOAD_AVRO_SCHEMA = "payload.avro.schema"
  private val PARAM_TASK_FILTER = "task.filter"
  private val PARAM_LOG_LEVEL = "log.level"
  private val PARAM_OPTION_SUBSCRIBE = "option.subscribe"

  private val PARAM_EXAMPLE_SHOULD_USE_SCHEMA_REGISTRY = "example.should.use.schema.registry"

  def main(args: Array[String]): Unit = {

    // there is an example file at /src/test/resources/AvroReadingExample.properties
    if (args.length != 1) {
      println("No properties file specified.")
      System.exit(1)
    }

    println("Loading properties from: " + args(0))
    val properties = ExamplesUtils.loadProperties(args(0))

    for (key <- properties.keysIterator) {
      println(s"\t${key} = ${properties.getProperty(key)}")
    }

    val spark = SparkSession
      .builder()
      .appName(properties.getProperty(PARAM_JOB_NAME))
      .master(properties.getProperty(PARAM_JOB_MASTER))
      .getOrCreate()

    spark.sparkContext.setLogLevel(properties.getProperty(PARAM_LOG_LEVEL))

    import ExamplesUtils._

    val stream = spark
      .readStream
      .format("kafka")
      .addOptions(properties) // 1. this method will add the properties starting with "option."; 2. security options can be set in the properties file

    val deserialized = configureExample(stream, properties)

    val filter = properties.getProperty(PARAM_TASK_FILTER)
    println("Going to run filter: " + filter)

    deserialized.printSchema()
    deserialized
        //.selectExpr("cast (key as string)")
      //.filter(filter)
      .writeStream.format("console").start().awaitTermination()
  }

  private def configureExample(stream: DataStreamReader,props: Properties): Dataset[Row] = {
    import ExamplesUtils._
    import za.co.absa.abris.avro.AvroSerDe._
    if (props.getProperty(PARAM_EXAMPLE_SHOULD_USE_SCHEMA_REGISTRY).toBoolean) {
      stream.fromConfluentAvro("value", None, Some(props.getSchemaRegistryConfigurations(PARAM_OPTION_SUBSCRIBE)))(RETAIN_SELECTED_COLUMN_ONLY)
    }
    else {
      stream.fromConfluentAvro("value", Some(props.getProperty(PARAM_PAYLOAD_AVRO_SCHEMA)), None)(RETAIN_SELECTED_COLUMN_ONLY)
    }
  }
}