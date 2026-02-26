/*
 * Copyright 2024 ABSA Group Limited
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

import org.apache.avro.Schema
import org.apache.spark.sql.avro.{AbrisAvroDeserializer, SchemaConverters}
import org.apache.spark.sql.types.DataType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.examples.data.generation.TestSchemas

class DeserializationExceptionHandlerWithPayloadSpec extends AnyFlatSpec with Matchers {

  private val schema: Schema = AvroSchemaUtils.parse(TestSchemas.COMPLEX_SCHEMA_SPEC)
  private val dataType: DataType = SchemaConverters.toSqlType(schema).dataType
  private val deserializer = new AbrisAvroDeserializer(schema, dataType)

  it should "receive the raw payload on deserialization failure" in {
    val expectedPayload = Array[Byte](0x00, 0x01, 0x02, 0x03)
    var capturedPayload: Array[Byte] = null

    val handler = new DeserializationExceptionHandlerWithPayload {
      override def handleWithPayload(
        exception: Throwable,
        deserializer: AbrisAvroDeserializer,
        readerSchema: Schema,
        payload: Array[Byte]
      ): Any = {
        capturedPayload = payload
        null
      }

      override def handle(
        exception: Throwable,
        deserializer: AbrisAvroDeserializer,
        readerSchema: Schema
      ): Any = {
        fail("handle() should not be called when handleWithPayload() is available")
      }
    }

    handler.handleWithPayload(new RuntimeException("test"), deserializer, schema, expectedPayload)

    capturedPayload should not be null
    capturedPayload should equal(expectedPayload)
  }

  it should "still work as a DeserializationExceptionHandler" in {
    var handleCalled = false

    val handler = new DeserializationExceptionHandlerWithPayload {
      override def handleWithPayload(
        exception: Throwable,
        deserializer: AbrisAvroDeserializer,
        readerSchema: Schema,
        payload: Array[Byte]
      ): Any = null

      override def handle(
        exception: Throwable,
        deserializer: AbrisAvroDeserializer,
        readerSchema: Schema
      ): Any = {
        handleCalled = true
        null
      }
    }

    // When referenced as the base type, handle() is callable
    val baseRef: DeserializationExceptionHandler = handler
    baseRef.handle(new RuntimeException("test"), deserializer, schema)
    handleCalled shouldBe true
  }

  it should "dispatch to handleWithPayload when handler extends the new trait" in {
    var receivedPayload: Array[Byte] = null
    var legacyHandleCalled = false

    val payloadHandler = new DeserializationExceptionHandlerWithPayload {
      override def handleWithPayload(
        exception: Throwable,
        deserializer: AbrisAvroDeserializer,
        readerSchema: Schema,
        payload: Array[Byte]
      ): Any = {
        receivedPayload = payload
        null
      }

      override def handle(
        exception: Throwable,
        deserializer: AbrisAvroDeserializer,
        readerSchema: Schema
      ): Any = {
        legacyHandleCalled = true
        null
      }
    }

    val legacyHandler = new DeserializationExceptionHandler {
      override def handle(
        exception: Throwable,
        deserializer: AbrisAvroDeserializer,
        readerSchema: Schema
      ): Any = {
        legacyHandleCalled = true
        null
      }
    }

    val testPayload = Array[Byte](0xDE.toByte, 0xAD.toByte)
    val testException = new RuntimeException("corrupt")

    // Simulate the dispatch logic from AvroDataToCatalyst
    def dispatch(handler: DeserializationExceptionHandler, payload: Array[Byte], e: Throwable): Any = {
      handler match {
        case h: DeserializationExceptionHandlerWithPayload =>
          h.handleWithPayload(e, deserializer, schema, payload)
        case h =>
          h.handle(e, deserializer, schema)
      }
    }

    // Test payload-aware handler
    dispatch(payloadHandler, testPayload, testException)
    receivedPayload should equal(testPayload)
    legacyHandleCalled shouldBe false

    // Test legacy handler
    dispatch(legacyHandler, testPayload, testException)
    legacyHandleCalled shouldBe true
  }
}
