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

package za.co.absa.abris.avro.errors

import all_types.test.{NativeSimpleOuter, Nested}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.{AbrisAvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.examples.data.generation.TestSchemas

class SpecificRecordExceptionHandlerSpec extends AnyFlatSpec {

  private val spark = SparkSession
    .builder()
    .appName("unitTest")
    .master("local[1]")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  it should "receive empty dataframe row back" in {
    // provided
    val providedDefaultRecord = NativeSimpleOuter.newBuilder()
      .setName("name")
      .setNested(Nested.newBuilder()
        .setInt$(1)
        .setLong$(1)
        .build())
      .build()

    // expected
    val expectedNestedFieldSchema = new StructType()
      .add("int", "int")
      .add("long", "long")
    val expectedNestedStructSchema = new StructType()
      .add("name", "string")
      .add("nested", expectedNestedFieldSchema)

    val expectedNestedFieldInternalRow = new SpecificInternalRow(expectedNestedFieldSchema)
    expectedNestedFieldInternalRow.setInt(0, 1)
    expectedNestedFieldInternalRow.setLong(1, 1L)

    val expectedNestedStructInternalRow = new SpecificInternalRow(expectedNestedStructSchema)
    expectedNestedStructInternalRow.update(0, UTF8String.fromString("name"))
    expectedNestedStructInternalRow.update(1, expectedNestedFieldInternalRow)

    //actual
    val deserializationExceptionHandler = new SpecificRecordExceptionHandler(providedDefaultRecord)
    val schema = AvroSchemaUtils.parse(TestSchemas.NATIVE_SIMPLE_OUTER_SCHEMA)
    val dataType: DataType = SchemaConverters.toSqlType(schema).dataType

    val actualResult = deserializationExceptionHandler
      .handle(new Exception, new AbrisAvroDeserializer(schema, dataType), schema)

    assert(actualResult == expectedNestedStructInternalRow)
  }
}
