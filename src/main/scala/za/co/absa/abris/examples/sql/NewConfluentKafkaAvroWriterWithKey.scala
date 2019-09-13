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

package za.co.absa.abris.examples.sql

import java.util.Properties

import org.apache.avro.Schema
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, Encoder, Row}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.examples.data.generation.ComplexRecordsGenerator
import za.co.absa.abris.examples.utils.ExamplesUtils._

import scala.collection.JavaConverters._


object NewConfluentKafkaAvroWriterWithKey {

  private val PARAM_JOB_NAME = "job.name"
  private val PARAM_JOB_MASTER = "job.master"
  private val PARAM_PAYLOAD_AVRO_SCHEMA = "payload.avro.schema"
  private val PARAM_KEY_AVRO_SCHEMA = "key.avro.schema"
  private val PARAM_AVRO_RECORD_NAME = "avro.record.name"
  private val PARAM_AVRO_RECORD_NAMESPACE = "avro.record.namespace"
  private val PARAM_INFER_SCHEMA = "infer.schema"
  private val PARAM_LOG_LEVEL = "log.level"
  private val PARAM_TEST_DATA_ENTRIES = "test.data.entries"
  private val PARAM_EXECUTION_REPEAT = "execution.repeat"
  private val PARAM_NUM_PARTITIONS = "num.partitions"
  private val PARAM_TOPIC = "option.topic"

  def main(args: Array[String]): Unit = {

    // there is a sample properties file at /src/test/resources/DataframeWritingExample.properties
    checkArgs(args)

    val properties = loadProperties(args)

    val spark = getSparkSession(properties, PARAM_JOB_NAME, PARAM_JOB_MASTER, PARAM_LOG_LEVEL)

    spark.sparkContext.setLogLevel(properties.getProperty(PARAM_LOG_LEVEL))

    import spark.implicits._

    implicit val encoder: Encoder[Row] = getEncoder(properties)

    do {
      val rows = createRows(properties.getProperty(PARAM_TEST_DATA_ENTRIES).trim().toInt)
      val dataFrame = spark.sparkContext.parallelize(rows, properties.getProperty(PARAM_NUM_PARTITIONS).toInt).toDF()

      dataFrame.show(false)

      toAvro(dataFrame, properties.asScala.toMap) // check the method content to understand how the library is invoked
        .write
        .format("kafka")
        .addOptions(properties) // 1. this method will add the properties starting with "option."
        .save()                 // 2. security options can be set in the properties file
    } while (properties.getProperty(PARAM_EXECUTION_REPEAT).toBoolean)
  }

  private def toAvro(dataFrame: Dataset[Row], properties: Map[String, String]) = {

    val commonRegistryConfig = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> properties(PARAM_TOPIC),
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> properties(SchemaManager.PARAM_SCHEMA_REGISTRY_URL),
      SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> properties(PARAM_AVRO_RECORD_NAME),
      SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> properties(PARAM_AVRO_RECORD_NAMESPACE)
    )

    val valueRegistryConfig = commonRegistryConfig +
      (SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> properties(SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY))

    val keyRegistryConfig = commonRegistryConfig +
      (SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> properties(SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY))

    val inferSchema = properties(PARAM_INFER_SCHEMA).trim().toBoolean

    import za.co.absa.abris.avro.functions.to_confluent_avro

    if (inferSchema) {
      dataFrame.select(
        to_confluent_avro(col("key"), keyRegistryConfig) as 'key,
        to_confluent_avro(col("value"), valueRegistryConfig) as 'value)
    } else {
      val valueSchema = loadSchemaFromFile(properties(PARAM_PAYLOAD_AVRO_SCHEMA))
      val keySchema = loadSchemaFromFile(properties(PARAM_KEY_AVRO_SCHEMA))

      dataFrame.select(
        to_confluent_avro(col("key"), keySchema, keyRegistryConfig) as 'key,
        to_confluent_avro(col("value"), valueSchema, valueRegistryConfig) as 'value)
    }
  }

  private def createRows(howMany: Int): List[Row] = {
    var count = 0
    ComplexRecordsGenerator
      .generateUnparsedRows(howMany)
      .map(row => {
        count = count + 1
        Row(Row(count, s"whatever string $count"),row)
      })
  }

  private def getEncoder(properties: Properties): Encoder[Row] = {
    val avroSchema = AvroSchemaUtils.parse(ComplexRecordsGenerator.usedAvroSchema)
    val payloadSparkSchema = SparkAvroConversions.toSqlType(avroSchema)

    val avroSchemas = getKeyAndPayloadSchemas(properties)

    val keySparkSchema = StructField("key", SparkAvroConversions.toSqlType(avroSchemas._1), nullable = false)
    val valueSparkSchema = StructField("value", payloadSparkSchema, nullable = false)

    val finalSchema = StructType(Array(keySparkSchema, valueSparkSchema))

    RowEncoder.apply(finalSchema)
  }

  private def getKeyAndPayloadSchemas(properties: Properties): (Schema,Schema) = {
    val keyAvroSchema = AvroSchemaUtils.load(properties.getProperty(PARAM_KEY_AVRO_SCHEMA))
    val payloadAvroSchema = AvroSchemaUtils.load(properties.getProperty(PARAM_PAYLOAD_AVRO_SCHEMA))

    (keyAvroSchema, payloadAvroSchema)
  }

  private def loadSchemaFromFile(path: String): String = {
    val source = scala.io.Source.fromFile(path)
    try source.mkString finally source.close()
  }
}
