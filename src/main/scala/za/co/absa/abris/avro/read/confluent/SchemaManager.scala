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

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata, SchemaRegistryClient}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializerConfig}
import org.apache.avro.Schema
import org.apache.kafka.common.config.ConfigException
import org.apache.spark.internal.Logging
import za.co.absa.abris.avro.subject.SubjectNameStrategyAdapterFactory

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
  * This object provides methods to integrate with remote schemas through Schema Registry.
  *
  * This can be considered an "enriched" facade to the Schema Registry client.
  *
  * This is NOT THREAD SAFE, which means that multiple threads operating on this object
  * (e.g. calling 'configureSchemaRegistry' with different parameters) would operated
  * on the same Schema Registry client, thus, leading to inconsistent behavior.
  */
object SchemaManager extends Logging {

  val PARAM_SCHEMA_REGISTRY_TOPIC = "schema.registry.topic"
  val PARAM_SCHEMA_REGISTRY_URL   = "schema.registry.url"
  val PARAM_VALUE_SCHEMA_ID       = "value.schema.id"
  val PARAM_KEY_SCHEMA_ID         = "key.schema.id"
  val PARAM_SCHEMA_ID_LATEST_NAME = "latest"

  val PARAM_KEY_SCHEMA_NAMING_STRATEGY   = "key.schema.naming.strategy"
  val PARAM_VALUE_SCHEMA_NAMING_STRATEGY = "value.schema.naming.strategy"

  val PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY      = "schema.name"
  val PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY = "schema.namespace"

  object SchemaStorageNamingStrategies extends Enumeration {
    val TOPIC_NAME        = "topic.name"
    val RECORD_NAME       = "record.name"
    val TOPIC_RECORD_NAME = "topic.record.name"
  }

  private var schemaRegistryClient: SchemaRegistryClient = _

  /**
    * Confluent's Schema Registry supports schemas for Kafka keys and values. What makes them different is simply the
    * what is appended to the schema name, either '-key' or '-value'.
    *
    * This method returns the subject name based on the topic and to which part of the message it corresponds.
    */
  def getSubjectName(topic: String, isKey: Boolean, schema: Schema, params: Map[String, String]): String = {
    val adapter = getSubjectNamingStrategyAdapter(isKey, params)

    if (adapter.validate(schema)) {
      val subjectName = adapter.subjectName(topic, isKey, schema)
      logDebug(s"Subject name resolved to: $subjectName")
      subjectName
    }
    else {
      throw new SchemaManagerException(
        s"Invalid configuration for naming strategy. Are you using RecordName or TopicRecordName? " +
        s"If yes, are you providing SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY and " +
        s"SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY in the configuration map?")
    }
  }

  def getSubjectName(topic: String, isKey: Boolean, schemaNameAndSpace: (String,String), params: Map[String, String]):
      String = {
    val (name, namespace) = schemaNameAndSpace
    getSubjectName(topic, isKey, Schema.createRecord(name, "", namespace, false), params)
  }

  private def getSubjectNamingStrategyAdapter(isKey: Boolean, params: Map[String,String]) = {
    val strategy = if (isKey) {
      params.getOrElse(
        PARAM_KEY_SCHEMA_NAMING_STRATEGY,
        throw new IllegalArgumentException(s"Parameter not specified: '$PARAM_KEY_SCHEMA_NAMING_STRATEGY'"))
    }
    else {
      params.getOrElse(
        PARAM_VALUE_SCHEMA_NAMING_STRATEGY,
        throw new IllegalArgumentException(s"Parameter not specified: '$PARAM_VALUE_SCHEMA_NAMING_STRATEGY'"))
    }
    SubjectNameStrategyAdapterFactory.build(strategy)
  }

  /**
    * Retrieves an Avro Schema instance from a given subject and stored with a given id.
    */
  def getBySubjectAndId(subject: String, id: Int): Schema = {
    logDebug(s"Trying to get schema for subject '$subject' and id '$id'")
    throwIfClientNotConfigured()

    Try(schemaRegistryClient.getBySubjectAndId(subject, id)) match {
      case Success(schema)  => schema
      case Failure(e)   => throw new SchemaManagerException(
        s"Could not get schema for subject '$subject' and id '$id'", e)
    }
  }

  /**
    * Retrieves an Avro [[SchemaMetadata]] instance from a given subject and stored with a given version.
    */
  def getBySubjectAndVersion(subject: String, version: Int): SchemaMetadata = {
    logDebug(s"Trying to get schema for subject '$subject' and version '$version'")
    throwIfClientNotConfigured()

    Try(schemaRegistryClient.getSchemaMetadata(subject, version)) match {
      case Success(id)  => id
      case Failure(e)   => throw new SchemaManagerException(
        s"Could not get schema metadata for subject '$subject' and version '$version'", e)
    }
  }

  def getById(id: Int): Schema = {
    logDebug(s"Trying to get schema for id '$id'")
    throwIfClientNotConfigured()

    Try(schemaRegistryClient.getById(id)) match {
      case Success(retrievedId)  => retrievedId
      case Failure(e)   => throw new SchemaManagerException(s"Could not get schema for id '$id'", e)
    }
  }

  /**
    * Retrieves the id corresponding to the latest schema available in Schema Registry.
    */
  def getLatestVersionId(subject: String): Int = {
    logDebug(s"Trying to get latest schema version id for subject '$subject'")
    throwIfClientNotConfigured()

    Try(schemaRegistryClient.getLatestSchemaMetadata(subject).getId) match {
      case Success(id)  => id
      case Failure(e)   => throw new SchemaManagerException(
        s"Could not get the id of the latest version for subject '$subject'", e)
    }
  }

  /**
    * Registers a schema into a given subject, returning the id the registration received.
    *
    * Afterwards the schema can be identified by this id.
    */
  def register(schema: Schema, subject: String): Int = {
    throwIfClientNotConfigured()
    schemaRegistryClient.register(subject, schema)
  }

  /**
    * Checks if SchemaRegistry has been configured, i.e. if it is null
    */
  def isSchemaRegistryConfigured: Boolean = schemaRegistryClient != null

  private def throwIfClientNotConfigured(): Unit = {
    if(!isSchemaRegistryConfigured) {
      throw new SchemaManagerException(s"Schema registry client not configured!")
    }
  }

  /**
   * Configures the Schema Registry client.
   * When invoked, it expects at least [[SchemaManager.PARAM_SCHEMA_REGISTRY_URL]] to be set.
   */
  def configureSchemaRegistry(configs: Map[String,String]): Unit = {
    if (configs.isEmpty) {
      logWarning(msg = "Asked to configure Schema Registry client but settings map is empty.")
    } else if (null != schemaRegistryClient) {
      logWarning(msg = "Schema Registry client is already configured.")
    } else {
      val settings = new KafkaAvroDeserializerConfig(configs.asJava)

      val urls = settings.getSchemaRegistryUrls
      val maxSchemaObject = settings.getMaxSchemasPerSubject

      logInfo(msg = s"Configuring new Schema Registry instance of type " +
        s"'${classOf[CachedSchemaRegistryClient].getCanonicalName}'")

      try {
        schemaRegistryClient = new CachedSchemaRegistryClient(urls, maxSchemaObject, configs.asJava)

      } catch {
        case e: io.confluent.common.config.ConfigException => throw new ConfigException(e.getMessage)
      }
    }
  }

  /**
    * This class uses [[CachedSchemaRegistryClient]] by default. This method can override the default.
    *
    * The incoming instance MUST be already configured.
    *
    * Useful for tests using mocked SchemaRegistryClient instances.
    */
  def setConfiguredSchemaRegistry(schemaRegistryClient: SchemaRegistryClient): Unit = {
    this.schemaRegistryClient = schemaRegistryClient
  }

  /**
    * Checks if a new schema is compatible with the latest schema registered for a given subject.
    */
  def isCompatible(newSchema: Schema, subject: String): Boolean = {
    this.schemaRegistryClient.testCompatibility(subject, newSchema)
  }

  /**
    * Checks if a given schema exists in Schema Registry.
    */
  def exists(subject: String): Boolean = {
    throwIfClientNotConfigured()

    try {
      schemaRegistryClient.getLatestSchemaMetadata(subject)
      true
    }
    catch {
      case e: Exception => {
        if (e.getMessage.contains("Subject not found") || e.getMessage.contains("No schema registered")) {
          logInfo(s"Subject not registered: '$subject'")
        }
        else {
          logError(s"Problems found while retrieving metadata for subject '$subject'", e)
        }
        false
      }
    }
  }

  /**
    * Resets this manager to its initial state, before being configured.
    */
  def reset(): Unit = schemaRegistryClient = null
}

class SchemaManagerException(msg: String, throwable: Throwable) extends RuntimeException(msg, throwable) {
  def this(msg: String) = this(msg, null)
}
