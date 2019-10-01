/*
 * Copyright 2019 ABSA Group Limited
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

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import za.co.absa.abris.examples.utils.ExamplesUtils._

object ConfluentKafkaAvroReader {

  private val PARAM_JOB_NAME = "job.name"
  private val PARAM_JOB_MASTER = "job.master"
  private val PARAM_PAYLOAD_AVRO_SCHEMA = "payload.avro.schema"
  private val PARAM_LOG_LEVEL = "log.level"
  private val PARAM_OPTION_SUBSCRIBE = "option.subscribe"

  private val PARAM_EXAMPLE_SHOULD_USE_SCHEMA_REGISTRY = "example.should.use.schema.registry"

  def main(args: Array[String]): Unit = {

    // there is an example file at /src/test/resources/AvroReadingExample.properties
    checkArgs(args)

    val properties = loadProperties(args)

    val spark = getSparkSession(properties, PARAM_JOB_NAME, PARAM_JOB_MASTER, PARAM_LOG_LEVEL)

    val stream = spark
      .readStream
      .format("kafka")
      .option("startingOffsets", "earliest")
      .addOptions(properties) // 1. this method will add the properties starting with "option."
                              // 2. security options can be set in the properties file

    val deserialized = configureExample(stream.load(), properties)

    // YOUR OPERATIONS CAN GO HERE

    deserialized.printSchema()

    deserialized
      .writeStream
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  private def configureExample(dataFrame: DataFrame, properties: Properties): Dataset[Row] = {

    import za.co.absa.abris.avro.functions.from_confluent_avro

    val schemaRegistryConfig = properties.getSchemaRegistryConfigurations(PARAM_OPTION_SUBSCRIBE)

    if (properties.getProperty(PARAM_EXAMPLE_SHOULD_USE_SCHEMA_REGISTRY).toBoolean) {
      dataFrame.select(from_confluent_avro(col("value"), schemaRegistryConfig) as 'data).select("data.*")
    } else {
      val source = scala.io.Source.fromFile(properties.getProperty(PARAM_PAYLOAD_AVRO_SCHEMA))
      val schemaString = try source.mkString finally source.close()
      dataFrame.select(from_confluent_avro(col("value"), schemaString, schemaRegistryConfig) as 'data)
        .select("data.*")
    }
  }
}
