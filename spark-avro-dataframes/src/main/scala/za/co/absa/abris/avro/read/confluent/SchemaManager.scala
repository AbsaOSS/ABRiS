/*
 * Copyright 2018 Barclays Africa Group Limited
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

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializerConfig}
import org.apache.avro.Schema
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConverters._

/**
  * This object provides methods to integrate with remote schemas through Schema Registry.
  *
  * This can be considered an "enriched" facade to the Schema Registry client.
  *
  * This is NOT THREAD SAFE, which means that multiple threads operating on this object (e.g. calling 'configureSchemaRegistry'
  * with different parameters) would operated on the same Schema Registry client, thus, leading to inconsistent behavior.
  */
object SchemaManager {

  val PARAM_SCHEMA_REGISTRY_TOPIC = "schema.registry.topic"
  val PARAM_SCHEMA_REGISTRY_URL = "schema.registry.url"
  val PARAM_SCHEMA_ID = "schema.id"

  val MAGIC_BYTE = 0x0
  val SCHEMA_ID_SIZE_BYTES = 4

  private var schemaRegistry: SchemaRegistryClient = _

  /**
    * Confluent's Schema Registry supports schemas for Kafka keys and values. What makes them different is simply the
    * what is appended to the schema name, either '-key' or '-value'.
    *
    * This method returns the subject name based on the topic and to which part of the message it corresponds.
    */
  def getSubjectName(topic: String, isKey: Boolean): String = {
    if (isKey) {
      topic + "-key"
    } else {
      topic + "-value"
    }
  }

  /**
    * Configures the Schema Registry client.
    * When invoked, it expects at least [[SchemaManager.PARAM_SCHEMA_REGISTRY_URL]] to be set.
    */
  def configureSchemaRegistry(configs: Map[String,String]): Unit = {
    if (configs.nonEmpty) {
      configureSchemaRegistry(new KafkaAvroDeserializerConfig(configs.asJava))
    }
  }

  /**
    * Retrieves an Avro Schema instance from a given subject and stored with a given id.
    * It will return None if the Schema Registry client is not configured.
    */
  def getBySubjectAndId(subject: String, id: Int): Option[Schema] = {
    if (isSchemaRegistryConfigured()) Some(schemaRegistry.getBySubjectAndID(subject, id)) else None
  }

  /**
    * Checks if SchemaRegistry has been configured, i.e. if it is null
    */
  def isSchemaRegistryConfigured(): Boolean = schemaRegistry != null

  /**
    * Configures the Schema Registry client.
    * When invoked, it expects at least the [[SchemaManager.PARAM_SCHEMA_REGISTRY_URL]] to be set.
    */
  private def configureSchemaRegistry(config: AbstractKafkaAvroSerDeConfig) = {
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
}