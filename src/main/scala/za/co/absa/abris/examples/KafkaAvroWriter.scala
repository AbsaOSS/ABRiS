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

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.{Dataset, Encoder, Row}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.examples.data.generation.ComplexRecordsGenerator
import za.co.absa.abris.examples.utils.ExamplesUtils._

import scala.collection.JavaConverters._
import scala.collection.mutable

object KafkaAvroWriter {

  private val PARAM_JOB_NAME = "job.name"
  private val PARAM_JOB_MASTER = "job.master"
  private val PARAM_PAYLOAD_AVRO_SCHEMA = "payload.avro.schema"
  private val PARAM_KEY_AVRO_RECORD_NAME = "avro.key.record.name"
  private val PARAM_KEY_AVRO_RECORD_NAMESPACE = "avro.key.record.namespace"
  private val PARAM_VALUE_AVRO_RECORD_NAME = "avro.value.record.name"
  private val PARAM_VALUE_AVRO_RECORD_NAMESPACE = "avro.value.record.namespace"
  private val PARAM_INFER_SCHEMA = "infer.schema"
  private val PARAM_LOG_LEVEL = "log.level"
  private val PARAM_TEST_DATA_ENTRIES = "test.data.entries"
  private val PARAM_EXECUTION_REPEAT = "execution.repeat"
  private val PARAM_NUM_PARTITIONS = "num.partitions"
  private val PARAM_EXAMPLE_SHOULD_USE_SCHEMA_REGISTRY = "example.should.use.schema.registry"
  private val PARAM_TOPIC = "option.topic"

  def main(args: Array[String]): Unit = {

    // there is a sample properties file at /src/test/resources/DataframeWritingExample.properties
    checkArgs(args)

    val properties = loadProperties(args)

    val spark = getSparkSession(properties, PARAM_JOB_NAME, PARAM_JOB_MASTER, PARAM_LOG_LEVEL)

    import spark.implicits._

    implicit val encoder: Encoder[Row] = getEncoder

    do {
      val rows = createRows(properties.getProperty(PARAM_TEST_DATA_ENTRIES).trim().toInt)
      val dataFrame = spark.sparkContext.parallelize(rows, properties.getProperty(PARAM_NUM_PARTITIONS).toInt).toDF()
      dataFrame.show(false)
      toAvro(dataFrame, properties.asScala)
        .write
        .format("kafka")
        .addOptions(properties) // 1. this method will add the properties starting with "option."
        .save()                 // 2. security options can be set in the properties file

    } while (properties.getProperty(PARAM_EXECUTION_REPEAT).toBoolean)
  }

  private def toAvro(dataFrame: Dataset[Row], properties: mutable.Map[String, String]) = {

    val registryConfig = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> properties(PARAM_TOPIC),
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> properties(SchemaManager.PARAM_SCHEMA_REGISTRY_URL),
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> properties(SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY),
      SchemaManager.PARAM_KEY_SCHEMA_NAME_FOR_RECORD_STRATEGY -> properties(PARAM_VALUE_AVRO_RECORD_NAME),
      SchemaManager.PARAM_KEY_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> properties(PARAM_VALUE_AVRO_RECORD_NAMESPACE),
      SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY -> properties(PARAM_VALUE_AVRO_RECORD_NAME),
      SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> properties(PARAM_VALUE_AVRO_RECORD_NAMESPACE)
    )

    val source = scala.io.Source.fromFile(properties(PARAM_PAYLOAD_AVRO_SCHEMA))
    val schemaString = try source.mkString finally source.close()

    val useRegistry = properties(PARAM_EXAMPLE_SHOULD_USE_SCHEMA_REGISTRY).toBoolean
    val inferSchema = properties(PARAM_INFER_SCHEMA).trim().toBoolean

    // to serialize all columns in dataFrame we need to put them in a spark struct
    val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)

    import za.co.absa.abris.avro.functions._

    if (!useRegistry && !inferSchema) {
      dataFrame.select(to_avro(allColumns, schemaString) as 'value)

    } else if (!useRegistry && inferSchema) {
      dataFrame.select(to_avro(allColumns) as 'value)

    } else if (useRegistry && !inferSchema) {
      dataFrame.select(to_avro(allColumns, schemaString, registryConfig) as 'value)

    } else { // useRegistry && inferSchema
      dataFrame.select(to_avro(allColumns, registryConfig) as 'value)
    }
  }

  private def createRows(howMany: Int): List[Row] = {
    ComplexRecordsGenerator.generateUnparsedRows(howMany)
  }

  private def getEncoder: Encoder[Row] = {
    val avroSchema = AvroSchemaUtils.parse(ComplexRecordsGenerator.usedAvroSchema)
    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)
    RowEncoder.apply(sparkSchema)
  }
}
