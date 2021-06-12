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

import org.apache.avro.generic.GenericData.Record
import org.apache.spark.sql.avro.{AbrisAvroDeserializer, SchemaConverters}
import org.apache.spark.sql.types.DataType
import org.scalatest.flatspec.AnyFlatSpec
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.examples.data.generation.TestSchemas

class ExceptionHandlerSpec extends AnyFlatSpec {

  it should "receive empty dataframe row back" in {
    val deserializationExceptionHandler = new EmptyExceptionHandler
    val schema = AvroSchemaUtils.parse(TestSchemas.COMPLEX_SCHEMA_SPEC)
    val dataType: DataType = SchemaConverters.toSqlType(schema).dataType
    val deserializer = new AbrisAvroDeserializer(schema, dataType)
    val expectedEmptyRecord = deserializer.deserialize(new Record(schema))

    assert(deserializationExceptionHandler.handle(
      new Exception, new AbrisAvroDeserializer(schema, dataType), schema) == expectedEmptyRecord)
  }
}
