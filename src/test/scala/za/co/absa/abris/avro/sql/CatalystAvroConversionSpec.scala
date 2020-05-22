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

package za.co.absa.abris.avro.sql

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.functions._
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.{SchemaManager, SchemaManagerFactory}
import za.co.absa.abris.examples.data.generation.{ComplexRecordsGenerator, TestSchemas}

class CatalystAvroConversionSpec extends FlatSpec with Matchers with BeforeAndAfterEach
{
  private val spark = SparkSession
    .builder()
    .appName("unitTest")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  implicit val encoder: Encoder[Row] = getEncoder

  private val schemaRegistryConfig = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "test_topic",
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "http://dummy",
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> "topic.record.name",
    SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "native_complete",
    SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "all-types.test"
  )

  private val latestIdSchemaRegistryConfig = schemaRegistryConfig ++ Map(
    SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest"
  )

  private val latestVersionSchemaRegistryConfig = schemaRegistryConfig ++ Map(
    SchemaManager.PARAM_VALUE_SCHEMA_VERSION -> SchemaManager.PARAM_SCHEMA_ID_LATEST_NAME
  )

  private def getTestingDataFrame: DataFrame = {
    val rowsForTest = 6
    val rows = ComplexRecordsGenerator.generateUnparsedRows(rowsForTest)
    spark.sparkContext.parallelize(rows, 2).toDF()
  }

  override def beforeEach() {
    SchemaManagerFactory.setClientInstance(new MockSchemaRegistryClient())
  }

  val bareByteSchema = """{"type": "bytes"}""""

  it should "convert one type with bare schema to avro an back" in {

    val allDate: DataFrame = getTestingDataFrame
    val dataFrame: DataFrame = allDate.select('bytes)

    val avroBytes = dataFrame
      .select(to_avro('bytes, bareByteSchema) as 'avroBytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro('avroBytes, bareByteSchema) as 'bytes)

    shouldEqualByData(dataFrame, result)
  }

  val recordByteSchema = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "bytes",
     "fields":[
 		     {"name": "bytes", "type": "bytes" }
     ]
  }"""

  it should "convert one type inside a record to avro an back" in {

    val allData: DataFrame = getTestingDataFrame
    val dataFrame: DataFrame = allData.select(struct(allData("bytes")) as 'bytes)

    val avroBytes = dataFrame
      .select(to_avro('bytes, recordByteSchema) as 'avroBytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro('avroBytes, recordByteSchema) as 'bytes)

    shouldEqualByData(dataFrame, result)
  }

  val recordRecordByteSchema = """
  {
  "type": "record",
  "name": "wrapper",
  "fields": [
    {
      "name": "wrapped",
      "type":
        {
          "type": "record",
          "name": "key",
          "fields": [
            {
              "name": "bytes",
              "type": "bytes"
            }
          ]
        }
      }
    ]
  }"""

  it should "convert one type inside a record inside another record to avro an back" in {

    val allDate: DataFrame = getTestingDataFrame
    val dataFrame: DataFrame = allDate.select(struct(struct(allDate("bytes"))) as 'bytes)

    val avroBytes = dataFrame
      .select(to_avro('bytes, recordRecordByteSchema) as 'avroBytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro('avroBytes, recordRecordByteSchema) as 'bytes)

    shouldEqualByData(dataFrame, result)
  }

  val complexRecordRecordSchema = """
  {
  "type": "record",
  "name": "wrapper",
  "fields": [
    { "name": "string", "type": ["string", "null"] },
    { "name": "boolean", "type": ["boolean","null"] },
    {
      "name": "wrapped",
      "type":
        {
          "type": "record",
          "name": "key",
          "fields": [
            { "name": "bytes", "type": "bytes" },
            { "name": "int", "type": ["int", "null"] },
            { "name": "long", "type": ["long", "null"] }
          ]
        }
      }
    ]
  }"""

  it should "convert complex type hierarchy to avro an back" in {

    val allData: DataFrame = getTestingDataFrame
    val dataFrame: DataFrame = allData.select(
      struct(
        allData("string"),
        allData("boolean"),
        struct(
          allData("bytes"),
          allData("int"),
          allData("long")
        )
      )
      as 'bytes)

    val avroBytes = dataFrame
      .select(to_avro('bytes, complexRecordRecordSchema) as 'avroBytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro('avroBytes, complexRecordRecordSchema) as 'bytes)

    shouldEqualByData(dataFrame, result)
  }

  val stringSchema = """
  {
    "type": "record",
    "name": "key",
    "fields": [
      {
        "name": "some_field",
        "type": ["string", "null"]
      }
    ]
  }
  """

  it should "convert nullable type to avro and back" in {

    val allData: DataFrame = getTestingDataFrame
    val dataFrame: DataFrame = allData.select(struct(allData("string")) as 'input)

    val avroBytes = dataFrame
      .select(to_avro('input, stringSchema) as 'bytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro('bytes, stringSchema) as 'input)

    shouldEqualByData(dataFrame, result)
  }


  it should "convert all types of data to avro an back" in {

    val dataFrame: DataFrame = getTestingDataFrame

    val schemaString = ComplexRecordsGenerator.usedAvroSchema
    val result = dataFrame
      .select(struct(dataFrame.columns.head, dataFrame.columns.tail: _*) as 'input)
      .select(to_avro('input, schemaString) as 'bytes)
      .select(from_avro('bytes, schemaString) as 'result)
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }


  it should "generate avro schema from spark dataType" in {

    val allData: DataFrame = getTestingDataFrame
    val dataFrame = allData.drop("fixed") //schema generation for fixed type is not supported

    val schemaString = TestSchemas.NATIVE_COMPLETE_SCHEMA_WITHOUT_FIXED

    val result = dataFrame
      .select(struct(dataFrame.columns.head, dataFrame.columns.tail: _*) as 'input)
      .select(to_avro('input) as 'bytes)
      .select(from_avro('bytes, schemaString) as 'result)
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }

  it should "convert one type to avro an back using schema registry and schema generation" in {

    val dataFrame: DataFrame = getTestingDataFrame

    val avroBytes = dataFrame
      .select(to_avro('bytes, schemaRegistryConfig) as 'avroBytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro('avroBytes, latestIdSchemaRegistryConfig) as 'result)
      .select("result.*")

    shouldEqualByData(dataFrame.select('bytes), result)
  }

  it should "convert all types of data to avro an back using schema registry" in {

    val dataFrame: DataFrame = getTestingDataFrame
    val schemaString = ComplexRecordsGenerator.usedAvroSchema

    val avroBytes = dataFrame
      .select(struct(dataFrame.columns.head, dataFrame.columns.tail: _*) as 'input)
      .select(to_avro('input, schemaString, schemaRegistryConfig) as 'bytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro('bytes, latestIdSchemaRegistryConfig) as 'result)
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }

  it should "convert all types of data to avro an back using schema registry and schema generation" in {

    val dataFrame: DataFrame = getTestingDataFrame

    val avroBytes = dataFrame
      .select(struct(dataFrame.columns.head, dataFrame.columns.tail: _*) as 'input)
      .select(to_avro('input, schemaRegistryConfig) as 'bytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro('bytes, latestIdSchemaRegistryConfig) as 'result)
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }

  it should "convert all types of data to confluent avro an back using schema registry and schema generation" in {

    val dataFrame: DataFrame = getTestingDataFrame

    val avroBytes = dataFrame
      .select(struct(dataFrame.columns.head, dataFrame.columns.tail: _*) as 'input)
      .select(to_confluent_avro('input, schemaRegistryConfig) as 'bytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_confluent_avro('bytes, latestIdSchemaRegistryConfig) as 'result)
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }

  it should "convert all types of data to confluent avro an back using schema registry" in {

    val dataFrame: DataFrame = getTestingDataFrame
    val schemaString = ComplexRecordsGenerator.usedAvroSchema

    val avroBytes = dataFrame
      .select(struct(dataFrame.columns.head, dataFrame.columns.tail: _*) as 'input)
      .select(to_confluent_avro('input, schemaString, schemaRegistryConfig) as 'bytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_confluent_avro('bytes, schemaString, schemaRegistryConfig) as 'result)
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }

  it should "convert all types of data to confluent avro an back using schema registry and the latest version" in {

    val dataFrame: DataFrame = getTestingDataFrame

    val schemaString = ComplexRecordsGenerator.usedAvroSchema
    val schemaManager = SchemaManagerFactory.create(latestVersionSchemaRegistryConfig)
    schemaManager.register(schemaString)

    val avroBytes = dataFrame
      .select(struct(dataFrame.columns.head, dataFrame.columns.tail: _*) as 'input)
      .select(to_confluent_avro('input, latestVersionSchemaRegistryConfig) as 'bytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_confluent_avro('bytes, latestVersionSchemaRegistryConfig) as 'result)
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }

  private val schemaRegistryConfigForKey = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "test_topic",
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "dummy",
    SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> "topic.name",
    SchemaManager.PARAM_KEY_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "native_complete",
    SchemaManager.PARAM_KEY_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "all-types.test"
  )

  private val latestSchemaRegistryConfigForKey = schemaRegistryConfigForKey ++ Map(
    SchemaManager.PARAM_KEY_SCHEMA_ID -> "latest"
  )

  it should "convert all types of data to confluent avro an back using schema registry for key" in {

    val dataFrame: DataFrame = getTestingDataFrame
    val schemaString = ComplexRecordsGenerator.usedAvroSchema

    val avroBytes = dataFrame
      .select(struct(dataFrame.columns.head, dataFrame.columns.tail: _*) as 'input)
      .select(to_confluent_avro('input, schemaString, schemaRegistryConfigForKey) as 'bytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_confluent_avro('bytes, latestSchemaRegistryConfigForKey) as 'result)
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }

  private def getEncoder: Encoder[Row] = {
    val avroSchema = AvroSchemaUtils.parse(ComplexRecordsGenerator.usedAvroSchema)
    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)
    RowEncoder.apply(sparkSchema)
  }

}
