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

import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.functions._
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry.{ConfluentMockRegistryClient, SchemaSubject}
import za.co.absa.abris.config.AbrisConfig

class SchemaEvolutionSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach
{
  private val spark = SparkSession
    .builder()
    .appName("unitTest")
    .master("local[2]")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  private val dummyUrl = "dummyUrl"
  private val schemaRegistryConfig = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> dummyUrl)

  override def beforeEach(): Unit = {
    val mockedSchemaRegistryClient = new ConfluentMockRegistryClient()
    SchemaManagerFactory.addSRClientInstance(schemaRegistryConfig, mockedSchemaRegistryClient)
  }

  val recordByteSchema = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "record_name",
     "fields":[
         {"name": "int", "type":  ["int", "null"] }
     ]
  }"""

  val recordEvolvedByteSchema = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "record_name",
     "fields":[
         {"name": "int", "type": ["int", "null"] },
         {"name": "favorite_color", "type": "string", "default": "green"}
     ]
  }"""

  private def createTestData(avroSchema: String): DataFrame = {
    val testInts = Seq(42, 66, 77, 321, 789) // scalastyle:ignore
    val rows = testInts.map(i => Row.fromSeq(Seq(i)))
    val rdd = spark.sparkContext.parallelize(rows, 2)

    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)

    spark.createDataFrame(rdd, sparkSchema)
  }

  it should "convert to avro with old schema and back with evolved schema (providing the schema)" in {

    val allData = createTestData(recordByteSchema)
    val dataFrame: DataFrame = allData.select(struct(allData.col(allData.columns.head)) as "integers")

    val toCAConfig = AbrisConfig
      .toConfluentAvro
      .provideAndRegisterSchema(recordByteSchema)
      .usingTopicRecordNameStrategy("test_topic")
      .usingSchemaRegistry(dummyUrl)

    val avroBytes = dataFrame
      .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

    avroBytes.collect() // force evaluation

    val fromCAConfig = AbrisConfig
      .fromConfluentAvro
      .provideReaderSchema(recordEvolvedByteSchema)
      .usingSchemaRegistry(dummyUrl)

    val result = avroBytes
      .select(from_avro(col("avroBytes"), fromCAConfig)
        as "integersWithDefault")

    val expectedStruct = struct(allData.col(allData.columns.head), lit("green"))
    val expectedResult: DataFrame = allData.select(expectedStruct as "integersWithDefault")

    shouldEqualByData(expectedResult, result)
  }

  it should "convert to avro with old schema and back with evolved schema (all from schema registry)" in {

    val allData = createTestData(recordByteSchema)
    val dataFrame: DataFrame = allData.select(struct(allData.col(allData.columns.head)) as "integers")

    val toCAConfig = AbrisConfig
      .toConfluentAvro
      .provideAndRegisterSchema(recordByteSchema)
      .usingTopicRecordNameStrategy("test_topic")
      .usingSchemaRegistry(dummyUrl)

    val avroBytes = dataFrame.select(to_avro(col("integers"), toCAConfig) as "avroBytes")

    // To avoid race conditions between schema registration and reading the data are converted from spark to scala
    val avroRows = avroBytes.collect()

    val schemaManager = SchemaManagerFactory.create(schemaRegistryConfig)
    val subject = SchemaSubject.usingTopicRecordNameStrategy(
      "test_topic",
      "record_name",
      "all-types.test"
    )

    schemaManager.register(subject, recordEvolvedByteSchema)

    // Now when the last version of schema is registered, we will convert the data back to spark DataFrame
    val avroDF = spark.sparkContext.parallelize(avroRows.toIndexedSeq, 2)
    val outputAvro = spark.createDataFrame(avroDF, avroBytes.schema)

    val fromCAConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicRecordNameStrategy("test_topic", "record_name", "all-types.test")
      .usingSchemaRegistry(dummyUrl)

    val result = outputAvro.select(from_avro(col("avroBytes"), fromCAConfig) as "integersWithDefault")

    val expectedStruct = struct(allData.col(allData.columns.head), lit("green"))
    val expectedResult: DataFrame = allData.select(expectedStruct as "integersWithDefault")

    shouldEqualByData(expectedResult, result)
  }

  it should "convert to simple avro with old schema and back with evolved reader schema (providing the schema)" in {

    val allData = createTestData(recordByteSchema)
    val dataFrame: DataFrame = allData.select(struct(allData.col(allData.columns.head)) as "integers")

    // Serialize record with a writer schema
    val toCAConfig = AbrisConfig
      .toSimpleAvro
      .provideSchema(recordByteSchema)

    val avroBytes = dataFrame
      .select(to_avro(col("integers"), toCAConfig) as "avroBytes")

    avroBytes.collect() // force evaluation

    // Deserialize record specifying a reader and a writer schema
    // Avro will decode using the writer schema and then match with the
    // reader schema. Thus e.g. new fields with a default value will also show up.
    val fromCAConfig = AbrisConfig
      .fromSimpleAvro
      .provideSchema(recordEvolvedByteSchema)
      .withWriterSchema(recordByteSchema)

    val result = avroBytes
      .select(from_avro(col("avroBytes"), fromCAConfig)
        as "integersWithDefault")

    val expectedStruct = struct(allData.col(allData.columns.head), lit("green"))
    val expectedResult: DataFrame = allData.select(expectedStruct as "integersWithDefault")

    shouldEqualByData(expectedResult, result)
  }
}
