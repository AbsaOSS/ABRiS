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

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.examples.utils.ExamplesUtils._

import scala.collection.JavaConverters._

object ConfluentKafkaAvroReaderWithKey {

  private val PARAM_JOB_NAME = "job.name"
  private val PARAM_JOB_MASTER = "job.master"
  private val PARAM_PAYLOAD_AVRO_SCHEMA = "payload.avro.schema"
  private val PARAM_KEY_AVRO_SCHEMA = "key.avro.schema"
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
      .addOptions(properties) // 1. this method will add the properties starting with "option."
                              // 2. security options can be set in the properties file

    val deserialized = configureExample(stream.load(), properties.asScala.toMap)

    // YOUR OPERATIONS CAN GO HERE

    deserialized.printSchema()

    deserialized
      .writeStream
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  private def configureExample(dataFrame: DataFrame, properties: Map[String, String]): Dataset[Row] = {

    val commonRegistryConfig = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> properties(PARAM_OPTION_SUBSCRIBE),
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> properties(SchemaManager.PARAM_SCHEMA_REGISTRY_URL),
      SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY ->
        properties(SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY),
      SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY ->
        properties(SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY)
    )

    val valueRegistryConfig = commonRegistryConfig ++ Map(
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> properties(SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY),
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> properties(SchemaManager.PARAM_VALUE_SCHEMA_ID)
    )

    val keyRegistryConfig = commonRegistryConfig ++ Map(
        SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> properties(SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY),
        SchemaManager.PARAM_KEY_SCHEMA_ID -> properties(SchemaManager.PARAM_KEY_SCHEMA_ID)
    )

    import za.co.absa.abris.avro.functions.from_confluent_avro

    if (properties(PARAM_EXAMPLE_SHOULD_USE_SCHEMA_REGISTRY).toBoolean) {
      dataFrame.select(
        from_confluent_avro(col("key"), keyRegistryConfig) as 'key,
        from_confluent_avro(col("value"), valueRegistryConfig) as 'value)
    } else {
      val valueSchema = loadSchemaFromFile(properties(PARAM_PAYLOAD_AVRO_SCHEMA))
      val keySchema = loadSchemaFromFile(properties(PARAM_KEY_AVRO_SCHEMA))
      dataFrame.select(
        from_confluent_avro(col("key"), keySchema, keyRegistryConfig) as 'key,
        from_confluent_avro(col("value"), valueSchema, valueRegistryConfig) as 'value)
    }
  }

  private def loadSchemaFromFile(path: String): String = {
    val source = scala.io.Source.fromFile(path)
    try source.mkString finally source.close()
  }
}
