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

import org.apache.spark.SparkException
import org.apache.spark.sql.avro.{AbrisAvroDeserializer, SchemaConverters}
import org.apache.spark.sql.types.DataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.examples.data.generation.TestSchemas


class FailFastExceptionHandlerSpec extends AnyFlatSpec with Matchers {

  it should "should throw spark exception on error" in {

    val deserializationExceptionHandler = new FailFastExceptionHandler
    val schema = AvroSchemaUtils.parse(TestSchemas.COMPLEX_SCHEMA_SPEC)
    val dataType: DataType = SchemaConverters.toSqlType(schema).dataType
    val deserializer = new AbrisAvroDeserializer(schema, dataType)

    an[SparkException] should be thrownBy (deserializationExceptionHandler.handle(new Exception, deserializer, schema))
    val exceptionThrown = the[SparkException] thrownBy (deserializationExceptionHandler.handle(new Exception, deserializer, schema))
    exceptionThrown.getMessage should equal("Malformed records are detected in record parsing.")
  }
}
