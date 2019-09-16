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
import za.co.absa.abris.avro.read.confluent.SchemaManager
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
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "dummy",
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> "topic.record.name",
    SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "native_complete",
    SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "all-types.test",
    SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest"
  )

  private def getTestingDataframe: DataFrame = {
    val rowsForTest = 6
    val rows = ComplexRecordsGenerator.generateUnparsedRows(rowsForTest)
    spark.sparkContext.parallelize(rows, 2).toDF()
  }

  override def beforeEach() {
    SchemaManager.setConfiguredSchemaRegistry(new MockSchemaRegistryClient())
  }

  it should "convert one type to avro an back" in {

    val dataframe: DataFrame = getTestingDataframe

    val schemaString = TestSchemas.BYTES_SCHEMA_SPEC

    val avroBytes = dataframe
      .select(to_avro('bytes, schemaString) as 'avroBytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro('avroBytes, schemaString) as 'result)
      .select("result.*")

    shouldEqualByData(dataframe.select('bytes), result)
  }

  it should "convert all types of data to avro an back" in {

    val dataframe: DataFrame = getTestingDataframe

    val schemaString = ComplexRecordsGenerator.usedAvroSchema
    val result = dataframe
      .select(struct(dataframe.columns.head, dataframe.columns.tail: _*) as 'input)
      .select(to_avro('input, schemaString) as 'bytes)
      .select(from_avro('bytes, schemaString) as 'result)
      .select("result.*")

    shouldEqualByData(dataframe, result)
  }


  it should "generate avro schema from spark dataType" in {

    val allData: DataFrame = getTestingDataframe
    val dataframe = allData.drop("fixed") //schema generation for fixed type is not supported

    val schemaString = TestSchemas.NATIVE_COMPLETE_SCHEMA_WITHOUT_FIXED

    val result = dataframe
      .select(struct(dataframe.columns.head, dataframe.columns.tail: _*) as 'input)
      .select(to_avro('input) as 'bytes)
      .select(from_avro('bytes, schemaString) as 'result)
      .select("result.*")

    shouldEqualByData(dataframe, result)
  }

  it should "convert one type to avro an back using schema registry and schema generation" in {

    val dataframe: DataFrame = getTestingDataframe

    val avroBytes = dataframe
      .select(to_avro('bytes, schemaRegistryConfig) as 'avroBytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro('avroBytes, schemaRegistryConfig) as 'result)
      .select("result.*")

    shouldEqualByData(dataframe.select('bytes), result)
  }

  it should "convert all types of data to avro an back using schema registry" in {

    val dataframe: DataFrame = getTestingDataframe
    val schemaString = ComplexRecordsGenerator.usedAvroSchema

    val avroBytes = dataframe
      .select(struct(dataframe.columns.head, dataframe.columns.tail: _*) as 'input)
      .select(to_avro('input, schemaString, schemaRegistryConfig) as 'bytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro('bytes, schemaRegistryConfig) as 'result)
      .select("result.*")

    shouldEqualByData(dataframe, result)
  }

  it should "convert all types of data to avro an back using schema registry and schema generation" in {

    val dataframe: DataFrame = getTestingDataframe

    val avroBytes = dataframe
      .select(struct(dataframe.columns.head, dataframe.columns.tail: _*) as 'input)
      .select(to_avro('input, schemaRegistryConfig) as 'bytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_avro('bytes, schemaRegistryConfig) as 'result)
      .select("result.*")

    shouldEqualByData(dataframe, result)
  }

  it should "convert all types of data to confluent avro an back using schema registry and schema generation" in {

    val dataframe: DataFrame = getTestingDataframe

    val avroBytes = dataframe
      .select(struct(dataframe.columns.head, dataframe.columns.tail: _*) as 'input)
      .select(to_confluent_avro('input, schemaRegistryConfig) as 'bytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_confluent_avro('bytes, schemaRegistryConfig) as 'result)
      .select("result.*")

    shouldEqualByData(dataframe, result)
  }

  it should "convert all types of data to confluent avro an back using schema registry" in {

    val dataframe: DataFrame = getTestingDataframe
    val schemaString = ComplexRecordsGenerator.usedAvroSchema

    val avroBytes = dataframe
      .select(struct(dataframe.columns.head, dataframe.columns.tail: _*) as 'input)
      .select(to_confluent_avro('input, schemaString, schemaRegistryConfig) as 'bytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_confluent_avro('bytes, schemaString) as 'result)
      .select("result.*")

    shouldEqualByData(dataframe, result)
  }
  private val schemaRegistryConfigForKey = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "test_topic",
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "dummy",
    SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> "topic.name",
    SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "native_complete",
    SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "all-types.test",
    SchemaManager.PARAM_KEY_SCHEMA_ID -> "latest"
  )

  it should "convert all types of data to confluent avro an back using schema registry for key" in {

    val dataframe: DataFrame = getTestingDataframe
    val schemaString = ComplexRecordsGenerator.usedAvroSchema

    val avroBytes = dataframe
      .select(struct(dataframe.columns.head, dataframe.columns.tail: _*) as 'input)
      .select(to_confluent_avro('input, schemaString, schemaRegistryConfigForKey) as 'bytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_confluent_avro('bytes, schemaRegistryConfigForKey) as 'result)
      .select("result.*")

    shouldEqualByData(dataframe, result)
  }

  /**
   * assert that both dataframes contain the same data
   */
  private def shouldEqualByData(inputFrame: DataFrame, outputFrame: DataFrame): Unit = {

    def columnNames(frame: DataFrame) = frame.schema.fields.map(_.name)

    val inputColNames = columnNames(inputFrame)
    val outputColNames = columnNames(outputFrame)

    inputColNames shouldEqual outputColNames

    inputColNames.foreach(col => {
      val inputColumn = inputFrame.select(col).collect().map(row => row.toSeq.head)
      val outputColumn = outputFrame.select(col).collect().map(row => row.toSeq.head)

      for ((input, output ) <- inputColumn.zip(outputColumn)) {
        input shouldEqual output
      }
    })
  }

  private def getEncoder: Encoder[Row] = {
    val avroSchema = AvroSchemaUtils.parse(ComplexRecordsGenerator.usedAvroSchema)
    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)
    RowEncoder.apply(sparkSchema)
  }

}
