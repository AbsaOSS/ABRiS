/*
 * Copyright 2021 ABSA Group Limited
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

import org.apache.spark.SparkException
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.avro.errors.{DefaultExceptionHandler, EmptyExceptionHandler}
import za.co.absa.abris.avro.functions._
import za.co.absa.abris.avro.utils.AvroSchemaEncoder
import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig}
import za.co.absa.abris.examples.data.generation.TestSchemas

class AvroDataToCatalystSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  private val spark = SparkSession
    .builder()
    .appName("unitTest")
    .master("local[2]")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  private val avroSchemaEncoder = new AvroSchemaEncoder

  import spark.implicits._

  implicit val encoder: Encoder[Row] = avroSchemaEncoder.getEncoder



  it should "not print schema registry configs in the spark plan" in {
    val sensitiveData = "username:password"
    val schemaString = TestSchemas.NATIVE_SIMPLE_NESTED_SCHEMA
    val dummyUrl = "dummyUrl"

    val fromAvroConfig = FromAvroConfig()
      .withReaderSchema(schemaString)
      .withSchemaRegistryConfig(Map(
        AbrisConfig.SCHEMA_REGISTRY_URL -> dummyUrl,
        "basic.auth.user.info" -> sensitiveData
      ))

    val column = from_avro(col("avroBytes"), fromAvroConfig)
    column.expr.toString() should not include sensitiveData
  }

  it should "use the default schema converter by default" in {
    val schemaString = TestSchemas.NATIVE_SIMPLE_NESTED_SCHEMA
    val dummyUrl = "dummyUrl"
    val expectedDataType = StructType(Seq(
      StructField("int", IntegerType, nullable = false),
      StructField("long", LongType, nullable = false)
    ))

    val fromAvroConfig = FromAvroConfig()
      .withReaderSchema(schemaString)
      .withSchemaRegistryConfig(Map(
        AbrisConfig.SCHEMA_REGISTRY_URL -> dummyUrl
      ))

    val column = from_avro(col("avroBytes"), fromAvroConfig)
    column.expr.dataType shouldBe expectedDataType
  }

  it should "use a custom schema converter identified by the short name" in {
    val schemaString = TestSchemas.NATIVE_SIMPLE_NESTED_SCHEMA
    val dummyUrl = "dummyUrl"

    val fromAvroConfig = FromAvroConfig()
      .withReaderSchema(schemaString)
      .withSchemaRegistryConfig(Map(
        AbrisConfig.SCHEMA_REGISTRY_URL -> dummyUrl
      ))
      .withSchemaConverter(DummySchemaConverter.name)

    val column = from_avro(col("avroBytes"), fromAvroConfig)
    column.expr.dataType shouldBe DummySchemaConverter.dataType
  }

  it should "use a custom schema converter identified by the fully qualified name" in {
    val schemaString = TestSchemas.NATIVE_SIMPLE_NESTED_SCHEMA
    val dummyUrl = "dummyUrl"

    val fromAvroConfig = FromAvroConfig()
      .withReaderSchema(schemaString)
      .withSchemaRegistryConfig(Map(
        AbrisConfig.SCHEMA_REGISTRY_URL -> dummyUrl
      ))
      .withSchemaConverter("za.co.absa.abris.avro.sql.DummySchemaConverter")

    val column = from_avro(col("avroBytes"), fromAvroConfig)
    column.expr.dataType shouldBe DummySchemaConverter.dataType
  }

  it should "throw an error if the specified custom schema converter does not exist" in {
    val schemaString = TestSchemas.NATIVE_SIMPLE_NESTED_SCHEMA
    val dummyUrl = "dummyUrl"

    val fromAvroConfig = FromAvroConfig()
      .withReaderSchema(schemaString)
      .withSchemaRegistryConfig(Map(
        AbrisConfig.SCHEMA_REGISTRY_URL -> dummyUrl
      ))
      .withSchemaConverter("nonexistent")

    val ex = intercept[ClassNotFoundException](from_avro(col("avroBytes"), fromAvroConfig).expr.dataType)
    ex.getMessage should include ("nonexistent")
  }

  it should "be serializable" in {
    val schemaString = TestSchemas.NATIVE_SIMPLE_NESTED_SCHEMA
    val config = FromAvroConfig().withReaderSchema(schemaString)
    val avroDataToCatalyst = from_avro(col("col"), config).expr

    val javaSerializer = new JavaSerializer(new SparkConf())
    javaSerializer.newInstance().serialize(avroDataToCatalyst)

    val kryoSerializer = new KryoSerializer(new SparkConf())
    kryoSerializer.newInstance().serialize(avroDataToCatalyst)

    // test successful if no exception is thrown
  }

  it should "throw a spark exception" in {

    val rowData = "$£%^"
    val data = Seq(Row(rowData.getBytes()))
    val dataFrame: DataFrame = spark.sparkContext.parallelize(data, 2).toDF() as "bytes"

    val schemaString = TestSchemas.NATIVE_SIMPLE_NESTED_SCHEMA
    val dummyUrl = "dummyUrl"

    val fromConfig = AbrisConfig
      .fromConfluentAvro
      .provideReaderSchema(schemaString)
      .usingSchemaRegistry(dummyUrl)
      .withExceptionHandler(new DefaultExceptionHandler)

    the[SparkException] thrownBy dataFrame.select(from_avro(col("bytes"), fromConfig )).collect()
  }

  it should "receive empty dataframe row back" in {

    val rowData = "$£%^"
    val data = Seq(Row(rowData.getBytes()))
    val dataFrame: DataFrame = spark.sparkContext.parallelize(data, 2).toDF() as "bytes"

    val schemaString = TestSchemas.NATIVE_SIMPLE_NESTED_SCHEMA
    val dummyUrl = "dummyUrl"
    val deserializationExceptionHandler = new EmptyExceptionHandler

    val fromConfig = AbrisConfig
      .fromConfluentAvro
      .provideReaderSchema(schemaString)
      .usingSchemaRegistry(dummyUrl)
      .withExceptionHandler(deserializationExceptionHandler)

    val expectedResult = Array(Row(Row(0, 0)))

    assert(dataFrame.select(from_avro(col("bytes"), fromConfig)).collect() sameElements expectedResult)
  }
}
