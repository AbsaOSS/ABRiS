package za.co.absa.abris.avro.read.confluent

import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.serializers.{KafkaAvroDeserializerConfig, NonRecordContainer}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.{SpecificData, SpecificRecord}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.SerializationException
import org.codehaus.jackson.node.JsonNodeFactory
import za.co.absa.abris.avro.format.ScalaAvroRecord
import za.co.absa.abris.avro.read.ScalaDatumReader

import scala.collection.JavaConverters._


class ScalaConfluentKafkaAvroDeserializer {

  val SCHEMA_REGISTRY_SCHEMA_VERSION_PROP = "schema.registry.schema.version"

  private val decoderFactory = DecoderFactory.get()
  protected var useSpecificAvroReader = false
  private var readerSchemaCache = new ConcurrentHashMap[String, Schema]()

  private val schemaManager = new SchemaManager()

  /**
    * Sets properties for this deserializer without overriding the schema registry client itself.
    * Useful for testing, where a mock client is injected.
    */
  protected def configure(config: KafkaAvroDeserializerConfig) = {
    schemaManager.configureClientProperties(config)
    useSpecificAvroReader = config.getBoolean(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG)
  }

  def configure(configs: Map[String,Any], isKey: Boolean): Unit = {
    configure(new KafkaAvroDeserializerConfig(configs.asJava))
  }

  protected def deserializerConfig(props: Map[String, Any]): KafkaAvroDeserializerConfig = {
    try {
      return new KafkaAvroDeserializerConfig(props.asJava)
    }
    catch {
      case e: io.confluent.common.config.ConfigException => throw new ConfigException(e.getMessage())
    }
  }

/*  protected def deserializerConfig(props: VerifiableProperties): KafkaAvroDeserializerConfig = {
    try {
      return new KafkaAvroDeserializerConfig(props.props())
    }
    catch {
      case e: io.confluent.common.config.ConfigException => throw new ConfigException(e.getMessage())
    }
  }*/

  private def getByteBuffer(payload: Array[Byte]): ByteBuffer = {
    val buffer = ByteBuffer.wrap(payload)
    if (buffer.get() != SchemaManager.MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!")
    }
    return buffer
  }

  /**
    * Deserializes the payload without including schema information for primitive types, maps, and
    * arrays. Just the resulting deserialized object is returned.
    *
    * <p>This behavior is the norm for Decoders/Deserializers.
    *
    * @param payload serialized data
    * @return the deserialized object
    */
  def deserialize(payload: Array[Byte]): Object = {
    return deserialize(false, None, Some(false), payload, None);
  }

  /**
    * Just like single-parameter version but accepts an Avro schema to use for reading
    *
    * @param payload      serialized data
    * @param readerSchema schema to use for Avro read (optional, enables Avro projection)
    * @return the deserialized object
    */
  def deserialize(payload: Array[Byte], readerSchema: Schema): Object = {
    return deserialize(false, None, Some(false), payload, Some(readerSchema))
  }

  def deserialize(topic: String, payload: Array[Byte]): GenericContainer = {
    deserialize(true, Some(topic), Some(false), payload, None).asInstanceOf[GenericContainer]
  }

  // The Object return type is a bit messy, but this is the simplest way to have
  // flexible decoding and not duplicate deserialization code multiple times for different variants.
  protected def deserialize(includeSchemaAndVersion: Boolean, topic: Option[String], isKey: Option[Boolean],
                            payload: Array[Byte], readerSchema: Option[Schema]): Object = {
    // Even if the caller requests schema & version, if the payload is null we cannot include it.
    // The caller must handle this case.
    if (payload == null) {
      return null
    }

    var id = -1
    try {
      val buffer = getByteBuffer(payload)
      id = buffer.getInt()
      val subject = if (includeSchemaAndVersion) SchemaManager.getSubjectName(topic.get, isKey.get) else null
      val schema = if (readerSchema.isEmpty)schemaManager.getBySubjectAndId(subject, id) else readerSchema.get
      println("SCHEMA: "+schema)
      val length = buffer.limit() - 1 - SchemaManager.idSize
      var result: Object = null
      if (schema.getType.equals(Schema.Type.BYTES)) {
        val bytes = new Array[Byte](length)
        buffer.get(bytes, 0, length)
        result = bytes
      } else {
        val start = buffer.position() + buffer.arrayOffset()
        //val reader = getDatumReader(schema, readerSchema.get)
        val reader = getDatumReader(schema, schema)
        result = reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null))
      }

      if (includeSchemaAndVersion) {
        // Annotate the schema with the version. Note that we only do this if the schema +
        // version are requested, i.e. in Kafka Connect converters. This is critical because that
        // code *will not* rely on exact schema equality. Regular deserializers *must not* include
        // this information because it would return schemas which are not equivalent.
        //
        // Note, however, that we also do not fill in the connect.version field. This allows the
        // Converter to let a version provided by a Kafka Connect source take priority over the
        // schema registry's ordering (which is implicit by auto-registration time rather than
        // explicit from the Connector).
        val version = schemaManager.getVersion(subject, schema);
        if (schema.getType() == Schema.Type.UNION) {
          // Can't set additional properties on a union schema since it's just a list, so set it
          // on the first non-null entry

          val notNullMember = schema.getTypes.asScala.find {member => member.getType != Schema.Type.NULL}
          if (notNullMember.nonEmpty) {
            notNullMember.get.addProp(SCHEMA_REGISTRY_SCHEMA_VERSION_PROP, JsonNodeFactory.instance.numberNode(version))
          }

/*          for (memberSchema: Schema <- schema.getTypes) {
            if (memberSchema.getType() != Schema.Type.NULL) {
              memberSchema.addProp(SCHEMA_REGISTRY_SCHEMA_VERSION_PROP, JsonNodeFactory.instance.numberNode(version))
              break;
            }
          }*/
        } else {
          schema.addProp(SCHEMA_REGISTRY_SCHEMA_VERSION_PROP,
            JsonNodeFactory.instance.numberNode(version))
        }
        if (schema.getType().equals(Schema.Type.RECORD)) {
          return result
        } else {
          return new NonRecordContainer(schema, result)
        }
      } else {
        return result
      }
    } catch {
      case e: IOException         => throw new SerializationException("Error deserializing Avro message for id " + id, e)
      case e: RuntimeException    => throw new SerializationException("Error deserializing Avro message for id " + id, e)
      case e: RestClientException => throw new SerializationException("Error retrieving Avro schema for id " + id, e)
    }
  }

  private def getDatumReader(writerSchema: Schema, readerSchema: Schema): ScalaDatumReader[ScalaAvroRecord] = {
    val writerSchemaIsPrimitive = SchemaManager.getPrimitiveSchemas().valuesIterator.contains(writerSchema)
    // do not use SpecificDatumReader if writerSchema is a primitive
    if (useSpecificAvroReader && !writerSchemaIsPrimitive) {
      new ScalaDatumReader[ScalaAvroRecord](writerSchema,
        if (readerSchema != null) readerSchema else getReaderSchema(writerSchema))
    } else {
      if (readerSchema == null) {
        new ScalaDatumReader[ScalaAvroRecord](writerSchema)
      }
      else {
        return new ScalaDatumReader[ScalaAvroRecord](writerSchema, readerSchema)
      }
    }
  }

  private def getReaderSchema(writerSchema: Schema): Schema = {
    var readerSchema = readerSchemaCache.get(writerSchema.getFullName())
    if (readerSchema == null) {
      val readerClass = SpecificData.get().getClass(writerSchema).asInstanceOf[Class[SpecificRecord]]
      if (readerClass != null) {
        try {
          readerSchema = readerClass.newInstance().getSchema()
        } catch {
          case e: InstantiationException => throw new SerializationException(writerSchema.getFullName()
            + " specified by the "
            + "writers schema could not be instantiated to "
            + "find the readers schema.")
          case e: IllegalAccessException => throw new SerializationException(writerSchema.getFullName()
            + " specified by the "
            + "writers schema is not allowed to be instantiated "
            + "to find the readers schema.")
        }
        readerSchemaCache.put(writerSchema.getFullName(), readerSchema)
      } else {
        throw new SerializationException("Could not find class "
          + writerSchema.getFullName()
          + " specified in writer's schema whilst finding reader's "
          + "schema for a SpecificRecord.")
      }
    }
    return readerSchema
  }
}
