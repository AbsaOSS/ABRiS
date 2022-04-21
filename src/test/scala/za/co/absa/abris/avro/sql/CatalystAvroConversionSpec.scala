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

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.functions._
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.{ConfluentMockRegistryClient, SchemaSubject}
import za.co.absa.abris.avro.utils.AvroSchemaEncoder
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.abris.examples.data.generation.{ComplexRecordsGenerator, TestSchemas}

class CatalystAvroConversionSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach
{
  private val spark = SparkSession
    .builder()
    .appName("unitTest")
    .master("local[2]")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  private val avroSchemaEncoder = new AvroSchemaEncoder
  implicit val encoder: Encoder[Row] = avroSchemaEncoder.getEncoder

  private val dummyUrl = "dummyUrl"
  private val schemaRegistryConfig = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> dummyUrl)

  private def getTestingDataFrame: DataFrame = {
    val rowsForTest = 6
    val rows = ComplexRecordsGenerator.generateUnparsedRows(rowsForTest)
    spark.sparkContext.parallelize(rows, 2).toDF()
  }

  override def beforeEach(): Unit = {
    val mockedSchemaRegistryClient = new ConfluentMockRegistryClient()
    SchemaManagerFactory.addSRClientInstance(schemaRegistryConfig, mockedSchemaRegistryClient)
  }

  val bareByteSchema = """{"type": "bytes"}""""

  it should "convert one type with bare schema to avro an back" in {

    val allDate: DataFrame = getTestingDataFrame
    val dataFrame: DataFrame = allDate.select(col("bytes"))

    val avroBytes = dataFrame
      .select(to_avro(col("bytes"), bareByteSchema) as "avroBytes")

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro(col("avroBytes"), bareByteSchema) as "bytes")

    shouldEqualByData(dataFrame, result)
  }

  val stringTypeSchema = """["string", "null"]"""

  it should "convert string type with bare schema to avro an back" in {

    val allDate: DataFrame = getTestingDataFrame
    val dataFrame: DataFrame = allDate.select(col("string"))

    val avroBytes = dataFrame
      .select(to_avro(col("string"), stringTypeSchema) as "avroBytes")

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro(col("avroBytes"), stringTypeSchema) as "string")

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
    val dataFrame: DataFrame = allData.select(struct(allData("bytes")) as "bytes")

    val avroBytes = dataFrame
      .select(to_avro(col("bytes"), recordByteSchema) as "avroBytes")

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro(col("avroBytes"), recordByteSchema) as "bytes")

    shouldEqualByData(dataFrame, result)
  }

  val recordRecordByteSchema = """
  {
  "type": "record",
  "name": "wrapper",
  "fields": [
    {
      "name": "col1",
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
    val dataFrame: DataFrame = allDate.select(struct(struct(allDate("bytes"))) as "bytes")

    val avroBytes = dataFrame
      .select(to_avro(col("bytes"), recordRecordByteSchema) as "avroBytes")

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro(col("avroBytes"), recordRecordByteSchema) as "bytes")

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
      "name": "col3",
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
      as "bytes")

    val avroBytes = dataFrame
      .select(to_avro(col("bytes"), complexRecordRecordSchema) as "avroBytes")

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro(col("avroBytes"), complexRecordRecordSchema) as "bytes")

    shouldEqualByData(dataFrame, result)
  }

  val stringSchema = """
  {
    "type": "record",
    "name": "topLevelRecord",
    "fields": [
      {
        "name": "string",
        "type": ["string", "null"]
      }
    ]
  }
  """

  it should "convert nullable type to avro and back" in {

    val allData: DataFrame = getTestingDataFrame
    val dataFrame: DataFrame = allData.select(struct(allData("string")) as "input")

    val avroBytes = dataFrame
      .select(to_avro(col("input"), stringSchema) as "bytes")

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro(col("bytes"), stringSchema) as "input")

    shouldEqualByData(dataFrame, result)
  }


  it should "convert all types of data to avro an back" in {

    val dataFrame: DataFrame = getTestingDataFrame

    val schemaString = ComplexRecordsGenerator.usedAvroSchema
    val result = dataFrame
      .select(struct(dataFrame.columns.map(col).toIndexedSeq: _*) as "input")
      .select(to_avro(col("input"), schemaString) as "bytes")
      .select(from_avro(col("bytes"), schemaString) as "result")
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }


  it should "generate avro schema from spark dataType" in {

    val allData: DataFrame = getTestingDataFrame
    val dataFrame = allData.drop("fixed") //schema generation for fixed type is not supported

    val schemaString = TestSchemas.NATIVE_COMPLETE_SCHEMA_WITHOUT_FIXED

    val input = dataFrame
      .select(struct(dataFrame.columns.map(col).toIndexedSeq: _*) as "input")

    val inputSchema = AvroSchemaUtils.toAvroSchema(input, "input").toString

    val result = input
      .select(to_avro(col("input"), inputSchema) as "bytes")
      .select(from_avro(col("bytes"), schemaString) as "result")
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }

  it should "generate avro schema from spark dataType with just single type" in {

    val allData: DataFrame = getTestingDataFrame
    val inputSchema = AvroSchemaUtils.toAvroSchema(allData, "int").toString

    val result = allData
      .select(to_avro(col("int"), inputSchema) as "avroInt")
      .select(from_avro(col("avroInt"), inputSchema) as "int")

    shouldEqualByData(allData.select(col("int")), result)
  }

  it should "convert one type to avro an back using schema registry and schema generation" in {

    val dataFrame: DataFrame = getTestingDataFrame

    val toAvroConfig = AbrisConfig
      .toSimpleAvro
      .provideAndRegisterSchema(AvroSchemaUtils.toAvroSchema(dataFrame, "bytes").toString)
      .usingTopicNameStrategy("fooTopic")
      .usingSchemaRegistry(dummyUrl)

    val avroBytes = dataFrame
      .select(to_avro(col("bytes"), toAvroConfig) as "avroBytes")

    avroBytes.collect() // force evaluation

    val fromAvroConfig = AbrisConfig
      .fromSimpleAvro
      .downloadSchemaByLatestVersion
      .andTopicNameStrategy("fooTopic")
      .usingSchemaRegistry(dummyUrl)

    val result = avroBytes
      .select(from_avro(col("avroBytes"), fromAvroConfig) as "bytes")

    shouldEqualByData(dataFrame.select(col("bytes")), result)
  }

  it should "convert all types of data to avro an back using schema registry" in {

    val dataFrame: DataFrame = getTestingDataFrame
    val schemaString = ComplexRecordsGenerator.usedAvroSchema

    val toAvroConfig = AbrisConfig
      .toSimpleAvro
      .provideAndRegisterSchema(schemaString)
      .usingTopicRecordNameStrategy("fooTopic")
      .usingSchemaRegistry(dummyUrl)

    val avroBytes = dataFrame
      .select(struct(dataFrame.columns.map(col).toIndexedSeq: _*) as "input")
      .select(to_avro(col("input"), toAvroConfig) as "bytes")

    avroBytes.collect() // force evaluation

    val fromAvroConfig = AbrisConfig
      .fromSimpleAvro
      .downloadSchemaByLatestVersion
      .andTopicRecordNameStrategy("fooTopic", "NativeComplete", "all_types.test")
      .usingSchemaRegistry(dummyUrl)

    val result = avroBytes
      .select(from_avro(col("bytes"), fromAvroConfig) as "result")
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }

  it should "convert all types of data to avro an back using schema registry and schema generation" in {

    val dataFrame: DataFrame = getTestingDataFrame
    val inputFrame = dataFrame
      .select(struct(dataFrame.columns.map(col).toIndexedSeq: _*) as "input")

    val inputSchema = AvroSchemaUtils.toAvroSchema(
      inputFrame,
      "input",
      "native_complete",
      "all-types.test"
    ).toString

    val toAvroConfig = AbrisConfig
      .toSimpleAvro
      .provideAndRegisterSchema(inputSchema)
      .usingRecordNameStrategy()
      .usingSchemaRegistry(dummyUrl)

    val avroBytes = inputFrame
      .select(to_avro(col("input"), toAvroConfig) as "bytes")

    avroBytes.collect() // force evaluation

    val fromAvroConfig = AbrisConfig
      .fromSimpleAvro
      .downloadSchemaByLatestVersion
      .andRecordNameStrategy("native_complete", "all-types.test")
      .usingSchemaRegistry(dummyUrl)

    val result = avroBytes
      .select(from_avro(col("bytes"), fromAvroConfig) as "result")
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }

  it should "convert all types of data to confluent avro an back using schema registry and schema generation" in {

    val dataFrame: DataFrame = getTestingDataFrame
    val inputFrame = dataFrame
      .select(struct(dataFrame.columns.map(col).toIndexedSeq: _*) as "input")

    val inputSchema = AvroSchemaUtils.toAvroSchema(
      inputFrame,
      "input",
      "native_complete",
      "all-types.test"
    ).toString

    val toConfig = AbrisConfig
      .toConfluentAvro
      .provideAndRegisterSchema(inputSchema)
      .usingTopicRecordNameStrategy("topicName")
      .usingSchemaRegistry(dummyUrl)

    val avroBytes = inputFrame
      .select(to_avro(col("input"), toConfig) as "bytes")

    avroBytes.collect() // force evaluation

    val fromConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicRecordNameStrategy("topicName", "native_complete", "all-types.test")
      .usingSchemaRegistry(dummyUrl)


    val result = avroBytes
      .select(from_avro(col("bytes"), fromConfig) as "result")
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }

  it should "convert all types of data to confluent avro an back using schema registry" in {

    val dataFrame: DataFrame = getTestingDataFrame
    val schemaString = ComplexRecordsGenerator.usedAvroSchema

    val toConfig = AbrisConfig
      .toConfluentAvro
      .provideAndRegisterSchema(schemaString)
      .usingTopicRecordNameStrategy("topicName")
      .usingSchemaRegistry(dummyUrl)

    val avroBytes = dataFrame
      .select(struct(dataFrame.columns.map(col).toIndexedSeq: _*) as "input")
      .select(to_avro(col("input"), toConfig) as "bytes")

    avroBytes.collect() // force evaluation

    val fromConfig = AbrisConfig
      .fromConfluentAvro
      .provideReaderSchema(schemaString)
      .usingSchemaRegistry(dummyUrl)

    val result = avroBytes
      .select(from_avro(col("bytes"), fromConfig) as "result")
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }

  it should "convert all types of data to confluent avro an back using schema registry and the latest version" in {

    val dataFrame: DataFrame = getTestingDataFrame

    val schemaString = ComplexRecordsGenerator.usedAvroSchema
    val schemaManager = SchemaManagerFactory.create(schemaRegistryConfig)
    val subject = SchemaSubject.usingTopicNameStrategy("fooTopic")
    val schemaId = schemaManager.register(subject, schemaString)

    val toConfig = AbrisConfig
      .toConfluentAvro
      .downloadSchemaById(schemaId)
      .usingSchemaRegistry(dummyUrl)

    val avroBytes = dataFrame
      .select(struct(dataFrame.columns.map(col).toIndexedSeq: _*) as "input")
      .select(to_avro(col("input"), toConfig) as "bytes")

    avroBytes.collect() // force evaluation

    val fromConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy("fooTopic")
      .usingSchemaRegistry(dummyUrl)

    val result = avroBytes
      .select(from_avro(col("bytes"), fromConfig) as "result")
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }

  it should "convert all types of data to confluent avro an back using schema registry for key" in {

    val dataFrame: DataFrame = getTestingDataFrame
    val schemaString = ComplexRecordsGenerator.usedAvroSchema

    val toConfig = AbrisConfig
      .toConfluentAvro
      .provideAndRegisterSchema(schemaString)
      .usingTopicNameStrategy("foo", true)
      .usingSchemaRegistry(dummyUrl)

    val avroBytes = dataFrame
      .select(struct(dataFrame.columns.map(col).toIndexedSeq: _*) as "input")
      .select(to_avro(col("input"), toConfig) as "bytes")

    avroBytes.collect() // force evaluation

    val fromConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy("foo", true)
      .usingSchemaRegistry(dummyUrl)

    val result = avroBytes
      .select(from_avro(col("bytes"), fromConfig) as "result")
      .select("result.*")

    shouldEqualByData(dataFrame, result)
  }
}
