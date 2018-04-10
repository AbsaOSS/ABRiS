package za.co.absa.abris.avro.read.confluent

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.errors.SerializationException

object SchemaManager {

  val MAGIC_BYTE = 0x0
  val idSize = 4

  private val parser = new Schema.Parser()

  private val primitiveSchemas = Map[Any,Schema](
    "Null"    -> createPrimitiveSchema(parser, "null"),
    "Boolean" -> createPrimitiveSchema(parser, "boolean"),
    "Integer" -> createPrimitiveSchema(parser, "int"),
    "Long"    -> createPrimitiveSchema(parser, "long"),
    "Float"   -> createPrimitiveSchema(parser, "float"),
    "Double"  -> createPrimitiveSchema(parser, "double"),
    "String"  -> createPrimitiveSchema(parser, "string"),
    "Bytes"   -> createPrimitiveSchema(parser, "bytes")
  )

  private def createPrimitiveSchema(parser: Schema.Parser, dataType: String) = {
    val schemaString = String.format("{\"type\" : \"%s\"}", dataType)
    parser.parse(schemaString)
  }

  def getPrimitiveSchemas() = {
    primitiveSchemas
  }

  def getSubjectName(topic: String, isKey: Boolean): String = {
    if (isKey) {
      topic + "-key"
    } else {
      topic + "-value"
    }
  }
}

class SchemaManager {

  private var schemaRegistry: SchemaRegistryClient = _

  def configureClientProperties(config: AbstractKafkaAvroSerDeConfig) = {
    try {
      val urls = config.getSchemaRegistryUrls()
      val maxSchemaObject = config.getMaxSchemasPerSubject()

      if (null == schemaRegistry) {
        schemaRegistry = new CachedSchemaRegistryClient(urls, maxSchemaObject)
      }
    } catch {
      case e: io.confluent.common.config.ConfigException => throw new ConfigException(e.getMessage())
    }
  }

  def getOldSubjectName(value: Object): String = {
    value match {
      case o: GenericContainer => o.getSchema.getName + "-value"
      case default => throw new SerializationException("Primitive types are not supported yet");
    }
  }

  def getSchema(o: Object): Schema = {
    o match {
      case null                      => SchemaManager.primitiveSchemas("Null")
      case o: java.lang.Boolean      => SchemaManager.primitiveSchemas("Boolean")
      case o: java.lang.Integer      => SchemaManager.primitiveSchemas("Integer")
      case o: java.lang.Long         => SchemaManager.primitiveSchemas("Long")
      case o: java.lang.Float        => SchemaManager.primitiveSchemas("Float")
      case o: java.lang.Double       => SchemaManager.primitiveSchemas("Double")
      case o: java.lang.CharSequence => SchemaManager.primitiveSchemas("String")
      case o: Array[Byte]            => SchemaManager.primitiveSchemas("Bytes")
      case o: GenericContainer       => o.getSchema
      case default => throw new IllegalArgumentException("Unsupported Avro type. Supported types are null, Boolean, Integer, Long, Float, Double, String, byte[] and IndexedRecord")
    }
  }

  def register(subject: String, schema: Schema): Int = {
    schemaRegistry.register(subject, schema)
  }

  def getById(id: Int): Schema = {
    schemaRegistry.getByID(id)
  }

  def getBySubjectAndId(subject: String, id: Int): Schema = {
    schemaRegistry.getBySubjectAndID(subject, id)
  }

  def getVersion(subject: String, schema: Schema) = {
    schemaRegistry.getVersion(subject, schema)
  }
}