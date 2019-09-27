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

package za.co.absa.abris.avro.read.confluent

import java.io.ByteArrayOutputStream
import java.lang.{Boolean, Double, Float, Long}
import java.nio.ByteBuffer

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.kafka.common.errors.SerializationException
import org.scalatest.{BeforeAndAfter, FlatSpec}
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.examples.data.generation.{AvroDataUtils, TestSchemas}

import scala.collection.immutable.Map

class ScalaConfluentKafkaAvroDeserializerSpec extends FlatSpec with BeforeAndAfter {

  private val plainSchema = TestSchemas.NATIVE_SCHEMA_SPEC
  private val schema = AvroSchemaUtils.parse(plainSchema)

  private val SCHEMA_ID = 8
  private val SCHEMA_SUBJECT = "FooBarBaz"

  private val testData = Map[String, Object](
    "string" ->  "A Test String",
    "float" ->   new Float(Float.MAX_VALUE),
    "int" ->     new Integer(Integer.MAX_VALUE),
    "long" ->    new Long(Long.MAX_VALUE),
    "double" ->  new Double(Double.MAX_VALUE),
    "boolean" -> new Boolean(true))

  private var avroRecord: GenericRecord = _

  before {
    SchemaManager.reset()
    avroRecord = AvroDataUtils.mapToGenericRecord(testData, plainSchema)
  }

  behavior of new ScalaConfluentKafkaAvroDeserializer(None, Some(schema)).getClass.getName

  it should "throw at constructor if neither topic nor Schema is informed" in {
    assertThrows[IllegalArgumentException] {new ScalaConfluentKafkaAvroDeserializer(None, None)}
  }

  it should "deserialize Confluent's Avro records assuming constructor Schema was used by both, writer and reader" in {
    val deserializer = new ScalaConfluentKafkaAvroDeserializer(None, Some(schema))
    val deserializedRecord = deserializer.deserialize(addConfluentHeader(avroRecord, schema, 42))
    for (testDataEntry <- testData) {
      assert(testDataEntry._2 == deserializedRecord.get(testDataEntry._1))
    }
  }

  it should "deserialize Confluent's Avro records retrieving schema from Schema Registry using topic" in {
    val schemaRegistryClient = new MockSchemaRegistryClient()
    val schemaId = schemaRegistryClient.register(SCHEMA_SUBJECT, schema)

    val deserializer = new ScalaConfluentKafkaAvroDeserializer(Some("any_topic_since_this_is_mocked"), None)
    SchemaManager.setConfiguredSchemaRegistry(schemaRegistryClient)
    val deserializedRecord = deserializer.deserialize(addConfluentHeader(avroRecord, schema, schemaId))
    for (testDataEntry <- testData) {
      assert(testDataEntry._2 == deserializedRecord.get(testDataEntry._1))
    }
  }

  it should "throw when Avro records are not Confluent compliant" in {
    val deserializer = new ScalaConfluentKafkaAvroDeserializer(None, Some(schema))
    val avroRecordBytes = AvroDataUtils.recordToBytes(avroRecord)
    assertThrows[SerializationException] {deserializer.deserialize(avroRecordBytes)}
  }

  /**
    * The section below was extracted from: [[io.confluent.kafka.serializers.AbstractKafkaAvroSerializer]].deserialize()
    *
    * That class was not used directly since that is tightly coupled to Schema Registry client
    * through [[io.confluent.kafka.serializers.AbstractKafkaAvroSerDe]] and an integration would unnecessarily
    * bloat this unit test, since in case of any change in the Confluent header,
    * the deserializer under test is expected to fail anyway.
    */

  private def addConfluentHeader(record: GenericRecord, schema: Schema, schemaId: Int): Array[Byte] = {
    val out = getConfluentHeaderStream(schemaId)
    appendBinaryRecordToConfluentStream(record, out)
    out.toByteArray
  }

  private def getConfluentHeaderStream(schemaId: Int): ByteArrayOutputStream = {
    val out = new ByteArrayOutputStream()
    out.write(ConfluentConstants.MAGIC_BYTE)
    out.write(ByteBuffer.allocate(ConfluentConstants.SCHEMA_ID_SIZE_BYTES).putInt(schemaId).array)
    out
  }

  private def appendBinaryRecordToConfluentStream(record: GenericRecord, out: ByteArrayOutputStream): Unit = {
    val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
    val writer = new SpecificDatumWriter[GenericRecord](schema)
    writer.write(record, encoder)
    encoder.flush()
  }
}
