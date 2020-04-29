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
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.functions._
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManager

class SchemaEvolutionSpec extends FlatSpec with Matchers with BeforeAndAfterEach
{
  private val spark = SparkSession
    .builder()
    .appName("unitTest")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  private val schemaRegistryConfig = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "test_topic",
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "dummy",
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> "topic.record.name",
    SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "record_name",
    SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "all-types.test"
  )

  private val latestSchemaRegistryConfig = schemaRegistryConfig ++ Map(
    SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest"
  )

  override def beforeEach() {
    SchemaManager.setConfiguredSchemaRegistry(new MockSchemaRegistryClient())
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
    val dataFrame: DataFrame = allData.select(struct(allData.col(allData.columns.head)) as 'integers)

    val avroBytes = dataFrame
      .select(to_confluent_avro('integers, schemaRegistryConfig) as 'avroBytes)

    avroBytes.collect() // force evaluation

    val result = avroBytes
      .select(from_confluent_avro('avroBytes, recordEvolvedByteSchema, latestSchemaRegistryConfig)
        as 'integersWithDefault)

    val expectedStruct = struct(allData.col(allData.columns.head), lit("green"))
    val expectedResult: DataFrame = allData.select(expectedStruct as 'integersWithDefault)

    shouldEqualByData(expectedResult, result)
  }

  it should "convert to avro with old schema and back with evolved schema (all from schema registry)" in {

    val allData = createTestData(recordByteSchema)
    val dataFrame: DataFrame = allData.select(struct(allData.col(allData.columns.head)) as 'integers)

    val avroBytes = dataFrame
        .select(to_confluent_avro('integers, schemaRegistryConfig) as 'avroBytes)

    // To avoid race conditions between schema registration and reading the data are converted from spark to scala
    val avroRows = avroBytes.collect()

    AvroSchemaUtils.registerSchema(recordEvolvedByteSchema, latestSchemaRegistryConfig)

    // Now when the last version of schema is registered, we will convert the data back to spark DataFrame
    val avroDF = spark.sparkContext.parallelize(avroRows, 2)
    val outputAvro = spark.createDataFrame(avroDF, avroBytes.schema)

    val result = outputAvro
        .select(from_confluent_avro('avroBytes, latestSchemaRegistryConfig)
          as 'integersWithDefault)

    val expectedStruct = struct(allData.col(allData.columns.head), lit("green"))
    val expectedResult: DataFrame = allData.select(expectedStruct as 'integersWithDefault)

    shouldEqualByData(expectedResult, result)
  }
}
