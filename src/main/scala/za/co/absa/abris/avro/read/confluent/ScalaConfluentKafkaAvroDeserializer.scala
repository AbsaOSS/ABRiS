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

import java.nio.ByteBuffer

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.common.errors.SerializationException
import za.co.absa.abris.avro.format.ScalaAvroRecord
import za.co.absa.abris.avro.read.ScalaDatumReader

/**
  * This class provides methods to deserialize Confluent binary Avro records into Spark Rows with schemas.
  *
  * Please, invest some time in understanding how it works and above all, read the documentation for the method 'deserialize()'.
  */
class ScalaConfluentKafkaAvroDeserializer(val readerSchema: Schema) {

  private val decoderFactory = DecoderFactory.get()
  private val idSchemaReader = scala.collection.mutable.Map[Int,ScalaDatumReader[ScalaAvroRecord]]()

  /**
    * This class does not hold a Schema Registry client instance. Instead, it relies on SchemaManager. This, this method
    * configures the Schema Registry on SchemaManager.
    *
    * This is here as a utility, so that users do not need to invoke SchemaManager in their codes at any time.
    */
  def configureSchemaRegistry(configs: Map[String,String]): Unit = {
    if (configs.nonEmpty) {
      SchemaManager.configureSchemaRegistry(configs)
    }
  }

  /**
    * Converts the Avro binary payload into an Avro's GenericRecord.
    * Important highlights:
    *
    * 1. This uses the [[ScalaDatumReader]] to parse the bytes.
    * 2. This takes into account Confluent's specific metadata included in the payload (e.g. schema id), thus, it will
    *    not work on regular binary Avro records.
    * 3. If there is a topic defined in the constructor and access to Schema Registry is configured, the schema retrieved
    *    from the later will be considered the writer schema, otherwise, the reader schema passed to the constructor will
    *    be used as both, reader and writer (thus notice that either, topic or reader schema must be informed).
    * 4. The Avro DatumReader is cached based on the schema id, thus, if a new id is received as part of the payload, a new
    *    DatumReader will be created for that id, with a new schema being retrieved, iff the topic is informed and Schema
    *    Registry is configured.
    * 5. Although changes in the schema are supported, it is important to bear in mind that this class's main reason of
    *    existence is to parse GenericRecords that will be later converted into Spark Rows. This conversion relies on
    *    RowEncoders, which need to be instantiated once, outside this class. Thus, even though schema changes can be dealt
    *    with here, they cannot be translated into new RowEncoders, which could generate from exceptions to inconsistencies
    *    in the final data.
    *
    *    The only way to overcome the issue described in 5. is to change Spark code itself, which would then be able to
    *    change the RowEncoder instance on the fly as a new schema version is detected.
    */
  def deserialize(payload: Array[Byte]): GenericRecord = {
    // Even if the caller requests schema & version, if the payload is null we cannot include it.
    // The caller must handle this case.
    if (payload == null) {
      return null
    }

    var schemaId = -1
    try {
      val buffer = getByteBuffer(payload)

      schemaId = buffer.getInt()
      val writerSchema = getWriterSchema(schemaId)

      val length = buffer.limit() - 1 - ConfluentConstants.SCHEMA_ID_SIZE_BYTES
      val start = buffer.position() + buffer.arrayOffset()

      val reader = getDatumReader(writerSchema, readerSchema, schemaId)
      reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null))
    }
    catch {
      case e: RestClientException => throw new SerializationException("Error retrieving Avro schema for id " + schemaId, e)
      case default: Throwable     => throw new SerializationException("Error deserializing Avro message for id " + schemaId, default)
    }
  }

  /**
    * If there is a topic defined and the Schema Registry has been configured, the writer schema will be retrieved from
    * Schema Registry, otherwise, the reader schema passed on to the constructor will also the considered the writer's.
    */
  private def getWriterSchema(id: Int): Schema = {
    if (SchemaManager.isSchemaRegistryConfigured) {
      SchemaManager.getById(id).get
    }
    else {
      readerSchema
    }
  }

  /**
    * Converts the binary payload into a ByteBuffer.
    *
    * This code was copied from [[io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer]].
    */
  private def getByteBuffer(payload: Array[Byte]): ByteBuffer = {
    val buffer = ByteBuffer.wrap(payload)
    if (buffer.get() != ConfluentConstants.MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!")
    }
    buffer
  }

  /**
    * Retrieves a DatumReader for a given schema id.
    *
    * The DatumReader is cached based on the id, thus, whenever the id changes, a new DatumReader is created. Refer to
    * the documentation of [[ScalaConfluentKafkaAvroDeserializer.deserialize()]] to understand the implications of schema
    * changes.
    */
  private def getDatumReader(writerSchema: Schema, readerSchema: Schema, id: Int): ScalaDatumReader[ScalaAvroRecord] = {
    idSchemaReader.getOrElseUpdate(id, createDatumReader(writerSchema, readerSchema))
  }

  /**
    * Creates a DatumReader for given reader and writer Avro schemas.
    *
    * If the reader schema passed on to the constructor is undefined, the writer schema is also considered the reader one.
    */
  private def createDatumReader(writerSchema: Schema, readerSchema: Schema): ScalaDatumReader[ScalaAvroRecord] = {
    new ScalaDatumReader[ScalaAvroRecord](writerSchema, readerSchema)
  }
}
