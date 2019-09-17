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

package za.co.absa.abris.examples.deprecated

import java.util.Properties

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Dataset, Encoder, Row}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.examples.data.generation.ComplexRecordsGenerator
import za.co.absa.abris.examples.utils.ExamplesUtils._

object ConfluentKafkaAvroWriter {

  private val PARAM_JOB_NAME = "job.name"
  private val PARAM_JOB_MASTER = "job.master"
  private val PARAM_KEY_AVRO_SCHEMA = "key.avro.schema"
  private val PARAM_PAYLOAD_AVRO_SCHEMA = "payload.avro.schema"
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

    implicit val encoder: Encoder[Row] = getEncoder

    do {
      val rows = createRows(properties.getProperty(PARAM_TEST_DATA_ENTRIES).trim().toInt)
      val dataframe = spark.sparkContext.parallelize(rows, properties.getProperty(PARAM_NUM_PARTITIONS).toInt).toDF()

      dataframe.show()

      toAvro(dataframe, properties) // check the method content to understand how the library is invoked
        .write
        .format("kafka")
        .addOptions(properties) // 1. this method will add the properties starting with "option."; 2. security options can be set in the properties file
        .save()
    } while (properties.getProperty(PARAM_EXECUTION_REPEAT).toBoolean)
  }

  private def toAvro(dataframe: Dataset[Row], properties: Properties) = {

    val sc = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> properties.getProperty(SchemaManager.PARAM_SCHEMA_REGISTRY_URL),
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> properties.getProperty(SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY)
    )
    val topic = properties.getProperty(PARAM_TOPIC)

    import za.co.absa.abris.avro.AvroSerDe._
    if (properties.getProperty(PARAM_INFER_SCHEMA).trim().toBoolean) {
      // providing access to Schema Registry is mandatory
      dataframe.toConfluentAvro(topic, properties.getProperty(PARAM_AVRO_RECORD_NAME), properties.getProperty(PARAM_AVRO_RECORD_NAMESPACE))(sc)
    } else {
      dataframe.toConfluentAvro(topic, properties.getProperty(PARAM_PAYLOAD_AVRO_SCHEMA))(sc)
    }
  }

  private def createRows(howMany: Int): List[Row] = {
    ComplexRecordsGenerator.generateUnparsedRows(howMany)
  }

  private def getEncoder: Encoder[Row] = {
    val avroSchema = AvroSchemaUtils.parse(ComplexRecordsGenerator.usedAvroSchema)
    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)
    println(sparkSchema)
    RowEncoder.apply(sparkSchema)
  }
}
