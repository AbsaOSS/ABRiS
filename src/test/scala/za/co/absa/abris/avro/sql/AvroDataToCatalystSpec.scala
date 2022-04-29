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

import all_types.test.{Fixed, NativeComplete}
import org.apache.spark.SparkException
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.avro.errors.{FailFastExceptionHandler, SpecificRecordExceptionHandler}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.functions._
import za.co.absa.abris.avro.utils.AvroSchemaEncoder
import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig}
import za.co.absa.abris.examples.data.generation.TestSchemas

import java.util.Collections
import java.nio.ByteBuffer
import java.util
import scala.collection.JavaConverters._

class AvroDataToCatalystSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private val spark = SparkSession
    .builder()
    .appName("unitTest")
    .master("local[2]")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._

  private val avroSchemaEncoder = new AvroSchemaEncoder
  implicit private val encoder: Encoder[Row] = avroSchemaEncoder.getEncoder

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

  it should "throw a Spark exception when unable to deserialize " in {

    val providedData = Seq(Row("$£%^".getBytes()))
    val providedDataFrame: DataFrame = spark.sparkContext.parallelize(providedData, 2).toDF() as "bytes"

    val dummyUrl = "dummyUrl"
    val fromConfig = AbrisConfig
      .fromConfluentAvro
      .provideReaderSchema(TestSchemas.NATIVE_SIMPLE_NESTED_SCHEMA)
      .usingSchemaRegistry(dummyUrl)
      .withExceptionHandler(new FailFastExceptionHandler)

    the[SparkException] thrownBy providedDataFrame.select(from_avro(col("bytes"), fromConfig )).collect()
  }

  it should "replace undeserializable record with default SpecificRecord" in {
    // provided
    val providedData = Seq(
      Row("$£%^".getBytes())
    )
    val providedDataFrame: DataFrame = spark.sparkContext.parallelize(providedData, 2).toDF() as "bytes"

    val providedDefaultRecord = NativeComplete.newBuilder()
      .setBytes(ByteBuffer.wrap(Array[Byte](1,2,3)))
      .setString("default-record")
      .setInt$(1)
      .setLong$(2L)
      .setDouble$(3.0)
      .setFloat$(4.0F)
      .setBoolean$(true)
      .setArray(Collections.singletonList("arrayItem1"))
      .setMap(Collections.singletonMap[CharSequence, util.List[java.lang.Long]](
        "key1",
        Collections.singletonList[java.lang.Long](1L)))
      .setFixed(new Fixed(Array.fill[Byte](40){1}))
      .build()

    // expected
    val expectedData = Seq(
      Row(Array[Byte](1,2,3),
          "default-record",
          1,
          2L,
          3.0,
          4F,
          true,
          Collections.singletonList("arrayItem1"),
          Collections.singletonMap[CharSequence, util.List[java.lang.Long]](
            "key1",
            Collections.singletonList[java.lang.Long](1L)),
          Array.fill[Byte](40){1}
      )).asJava

    val expectedDataFrame: DataFrame = spark.createDataFrame(expectedData, SparkAvroConversions.toSqlType(NativeComplete.SCHEMA$))

    // actual
    val dummyUrl = "dummyUrl"
    val fromConfig = AbrisConfig
      .fromConfluentAvro
      .provideReaderSchema(NativeComplete.SCHEMA$.toString())
      .usingSchemaRegistry(dummyUrl)
      .withExceptionHandler(new SpecificRecordExceptionHandler(providedDefaultRecord))

    val actualDataFrame = providedDataFrame
      .select(from_avro(col("bytes"), fromConfig).as("actual"))
      .select(col("actual.*"))

    shouldEqualByData(expectedDataFrame, actualDataFrame)
  }
}
