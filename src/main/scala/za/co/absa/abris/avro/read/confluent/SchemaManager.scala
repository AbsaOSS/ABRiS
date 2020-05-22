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

import java.security.InvalidParameterException

import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.spark.internal.Logging
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManager.PARAM_SCHEMA_ID_LATEST_NAME
import za.co.absa.abris.avro.schemas.RegistryConfig

import scala.util.{Failure, Success, Try}

class SchemaManager(
   config: RegistryConfig,
   schemaRegistryClient: SchemaRegistryClient) extends Logging {

  lazy val schemaId: Option[Int] = resolveSchemaId()

  private def resolveSchemaId(): Option[Int] = {
    val subject = config.subjectName()

    config.schemaIdOption match {
      case Some(PARAM_SCHEMA_ID_LATEST_NAME) =>  Some(getLatestVersionId(subject))
      case Some(id) => Some(id.toInt)
      case _ => config.schemaVersionOption match {
        case Some(PARAM_SCHEMA_ID_LATEST_NAME) => Some(getLatestVersionId(subject))
        case Some(version) => Some(getSchemaMetadataBySubjectAndVersion(subject, version.toInt).getId)
        case _ => None
      }
    }
  }

  /**
   * Retrieves the id corresponding to the latest schema available in Schema Registry.
   */
  private def getLatestVersionId(subject: String): Int = {
    logDebug(s"Trying to get latest schema version id for subject '$subject'")

    Try(schemaRegistryClient.getLatestSchemaMetadata(subject).getId) match {
      case Success(id)  => id
      case Failure(e)   => throw new SchemaManagerException(
        s"Could not get the id of the latest version for subject '$subject'", e)
    }
  }

  /**
   * Retrieves an Avro [[SchemaMetadata]] instance from a given subject and stored with a given version.
   */
  private def getSchemaMetadataBySubjectAndVersion(subject: String, version: Int): SchemaMetadata = {
    logDebug(s"Trying to get schema for subject '$subject' and version '$version'")

    Try(schemaRegistryClient.getSchemaMetadata(subject, version)) match {
      case Success(id)  => id
      case Failure(e)   => throw new SchemaManagerException(
        s"Could not get schema metadata for subject '$subject' and version '$version'", e)
    }
  }

  def downloadSchema(): Schema = getBySubjectAndId(config.subjectName(), schemaId.get)

  /**
   * Retrieves an Avro Schema instance from a given subject and stored with a given id.
   */
  private def getBySubjectAndId(subject: String, id: Int): Schema = {
    logDebug(s"Trying to get schema for subject '$subject' and id '$id'")

    Try(schemaRegistryClient.getBySubjectAndId(subject, id)) match {
      case Success(schema)  => schema
      case Failure(e)   => throw new SchemaManagerException(
        s"Could not get schema for subject '$subject' and id '$id'", e)
    }
  }

  def downloadById(id: Int): Schema = {
    logDebug(s"Trying to get schema for id '$id'")

    Try(schemaRegistryClient.getById(id)) match {
      case Success(retrievedId)  => retrievedId
      case Failure(e)   => throw new SchemaManagerException(s"Could not get schema for id '$id'", e)
    }
  }

  def register(schemaString: String): Int = register(AvroSchemaUtils.parse(schemaString))

  /**
   * Register a new schema for a subject if the schema is compatible with the latest available version.
   *
   * @return registered schema id
   */
  def register(schema: Schema): Int = {
    val subject = config.subjectName(schema)

    if (!exists(subject) || isCompatible(schema, subject)) {
      logInfo(s"AvroSchemaUtils.registerIfCompatibleSchema: Registering schema for subject: $subject")
      schemaRegistryClient.register(subject, schema)
    } else {
      throw new InvalidParameterException(s"Schema could not be registered for subject '${subject}'. " +
        "Make sure that the Schema Registry is available, the parameters are correct and the schemas are compatible")
    }
  }

  /**
   * Checks if a given schema exists in Schema Registry.
   */
  private def exists(subject: String): Boolean = {
    Try(schemaRegistryClient.getLatestSchemaMetadata(subject)) match {
      case Success(_) => true
      case Failure(e) if e.getMessage.contains("Subject not found") || e.getMessage.contains("No schema registered") =>
        logInfo(s"Subject not registered: '$subject'")
        false
      case Failure(e) =>
        logError(s"Problems found while retrieving metadata for subject '$subject'", e)
        false
    }
  }

  /**
   * Checks if a new schema is compatible with the latest schema registered for a given subject.
   */
  private def isCompatible(newSchema: Schema, subject: String): Boolean = {
    schemaRegistryClient.testCompatibility(subject, newSchema)
  }
}

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
  val PARAM_SCHEMA_REGISTRY_URL = "schema.registry.url"
  val PARAM_VALUE_SCHEMA_ID = "value.schema.id"
  val PARAM_VALUE_SCHEMA_VERSION = "value.schema.version"
  val PARAM_KEY_SCHEMA_ID = "key.schema.id"
  val PARAM_KEY_SCHEMA_VERSION = "key.schema.version"
  val PARAM_SCHEMA_ID_LATEST_NAME = "latest"

  val PARAM_KEY_SCHEMA_NAMING_STRATEGY = "key.schema.naming.strategy"
  val PARAM_VALUE_SCHEMA_NAMING_STRATEGY = "value.schema.naming.strategy"

  val PARAM_KEY_SCHEMA_NAME_FOR_RECORD_STRATEGY = "key.schema.name"
  val PARAM_KEY_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY = "key.schema.namespace"

  val PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY = "value.schema.name"
  val PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY = "value.schema.namespace"

  object SchemaStorageNamingStrategies extends Enumeration {
    val TOPIC_NAME = "topic.name"
    val RECORD_NAME = "record.name"
    val TOPIC_RECORD_NAME = "topic.record.name"
  }
}

class SchemaManagerException(msg: String, throwable: Throwable) extends RuntimeException(msg, throwable) {
  def this(msg: String) = this(msg, null)
}
