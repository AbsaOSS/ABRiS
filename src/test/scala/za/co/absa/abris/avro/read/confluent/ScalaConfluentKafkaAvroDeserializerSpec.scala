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
import java.util

import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
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
    val deserializedRecord = deserializer.deserialize(addConfluentHeader(avroRecord, schema))
    for (testDataEntry <- testData) {
      assert(testDataEntry._2 == deserializedRecord.get(testDataEntry._1))
    }
  }

  it should "deserialize Confluent's Avro records retrieving schema from Schema Registry using topic" in {
    val deserializer = new ScalaConfluentKafkaAvroDeserializer(Some("any_topic_since_this_is_mocked"), None)
    SchemaManager.setConfiguredSchemaRegistry(new MockedSchemaRegistryClient)
    val deserializedRecord = deserializer.deserialize(addConfluentHeader(avroRecord, schema))
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
    * That class was not used directly since that is tightly coupled to Schema Registry client through [[io.confluent.kafka.serializers.AbstractKafkaAvroSerDe]]
    * and an integration would unnecessarily bloat this unit test, since in case of any change in the Confluent header,
    * the deserializer under test is expected fail anyway.
    */

  private def addConfluentHeader(record: GenericRecord, schema: Schema): Array[Byte] = {
    val out = getConfluentHeaderStream
    appendBinaryRecordToConfluentStream(record, out)
    out.toByteArray
  }

  private def getConfluentHeaderStream: ByteArrayOutputStream = {
    val out = new ByteArrayOutputStream()
    out.write(ConfluentConstants.MAGIC_BYTE)
    out.write(ByteBuffer.allocate(ConfluentConstants.SCHEMA_ID_SIZE_BYTES).putInt(8).array)
    out
  }

  private def appendBinaryRecordToConfluentStream(record: GenericRecord, out: ByteArrayOutputStream) = {
    val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
    val writer = new SpecificDatumWriter[GenericRecord](schema)
    writer.write(record, encoder)
    encoder.flush()
  }

  private class MockedSchemaRegistryClient extends SchemaRegistryClient {

    override def getVersion(s: String, schema: Schema): Int = 8

    override def getAllSubjects: util.Collection[String] = new util.ArrayList[String]()

    override def getSchemaMetadata(s: String, i: Int): SchemaMetadata = null

    override def getBySubjectAndID(s: String, i: Int): Schema = getBySubjectAndId(s, i)

    override def getById(id: Int): org.apache.avro.Schema = schema

    override def getBySubjectAndId(subject: String, id: Int): org.apache.avro.Schema = schema

    override def getLatestSchemaMetadata(s: String): SchemaMetadata = null

    override def updateCompatibility(s: String, s1: String): String = null

    override def getByID(i: Int): Schema = schema

    override def getCompatibility(s: String): String = null

    override def testCompatibility(s: String, schema: Schema) = true

    override def register(s: String, schema: Schema): Int = 8

    override def deleteSchemaVersion(conf: java.util.Map[String,String], subject: String, version: String): Integer = ???
    override def deleteSchemaVersion(subject: String, version: String): Integer = ???
    override def deleteSubject(params: java.util.Map[String,String], subject: String): java.util.List[Integer] = ???
    override def deleteSubject(subject: String): java.util.List[Integer] = ???
    override def getAllVersions(subject: String): java.util.List[Integer] = ???
    override def getId(subject: String, schema: org.apache.avro.Schema): Int = ???
  }
}