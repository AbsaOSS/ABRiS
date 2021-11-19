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

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import org.apache.avro.Schema
import org.apache.spark.internal.Logging
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.registry._

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

/**
 * This class provides methods to integrate with remote schemas through Schema Registry.
 *
 * This can be considered an "enriched" facade to the Schema Registry client.
 *
 * This is NOT THREAD SAFE, which means that multiple threads operating on this object
 * (e.g. calling 'configureSchemaRegistry' with different parameters) would operated
 * on the same Schema Registry client, thus, leading to inconsistent behavior.
 */
class SchemaManager(schemaRegistryClient: AbrisRegistryClient) extends Logging {

  def this(schemaRegistryClient: SchemaRegistryClient) = this(new ConfluentRegistryClient(schemaRegistryClient))

  def getSchema(coordinate: SchemaCoordinate): Schema = coordinate match {
      case IdCoordinate(id) => getSchemaById(id)
      case SubjectCoordinate(subject, version) => getSchemaBySubjectAndVersion(subject, version)
    }

  def getSchemaById(schemaId: Int): Schema = schemaRegistryClient.getById(schemaId)

  /**
   * @param version - Some(versionNumber) or None for latest version
   */
  def getSchemaBySubjectAndVersion(subject: SchemaSubject, version: SchemaVersion): Schema = {
    val metadata = getSchemaMetadataBySubjectAndVersion(subject, version)

    AvroSchemaUtils.parse(metadata.getSchema)
  }

  /**
   * @param version - Some(versionNumber) or None for latest version
   */
  def getSchemaMetadataBySubjectAndVersion(subject: SchemaSubject, version: SchemaVersion): SchemaMetadata =
    version match {
      case NumVersion(versionInt) => schemaRegistryClient.getSchemaMetadata(subject.asString, versionInt)
      case LatestVersion() => schemaRegistryClient.getLatestSchemaMetadata(subject.asString)
    }

  def register(subject: SchemaSubject, schemaString: String): Int =
    register(subject, AvroSchemaUtils.parse(schemaString))

  /**
   * Register new schema for a subject.
   *
   * @throws InvalidParameterException when the new schema is not compatible with already exiting one.
   * @return registered schema id
   */
  def register(subject: SchemaSubject, schema: Schema): Int = {
    if (!exists(subject) || isCompatible(schema, subject)) {
      logInfo(s"AvroSchemaUtils.registerIfCompatibleSchema: Registering schema for subject: $subject")
      schemaRegistryClient.register(subject.asString, schema)
    } else {
      throw new InvalidParameterException(s"Schema registration failed. Schema for subject:'$subject' " +
        s"already exists and it is not compatible with schema you are trying to register.")
    }
  }

  /**
   * Checks if a given schema exists in Schema Registry.
   */
  def exists(subject: SchemaSubject): Boolean = {
    Try(schemaRegistryClient.getLatestSchemaMetadata(subject.asString)) match {
      case Success(_) => true
      case Failure(e: RestClientException) if e.getStatus == 404 => false
      case Failure(e) => throw e
    }
  }

  /**
   * Checks if a new schema is compatible with the latest schema registered for a given subject.
   */
  private def isCompatible(newSchema: Schema, subject: SchemaSubject): Boolean = {
    schemaRegistryClient.testCompatibility(subject.asString, newSchema)
  }

  def getAllSchemasWithMetadata(subject: SchemaSubject): List[SchemaMetadata] = {
    val versions = Try(schemaRegistryClient.getAllVersions(subject.asString)) match {
      case Success(l) => l.asScala.toList
      case Failure(e: RestClientException) if e.getStatus == 404 => List.empty[Integer]
      case Failure(e) => throw e
    }

    versions.map(schemaRegistryClient.getSchemaMetadata(subject.asString, _))
  }

  def findEquivalentSchema(schema: Schema, subject: SchemaSubject): Option[Int] = {
    val maybeIdenticalSchemaMetadata =
      getAllSchemasWithMetadata(subject)
        .find{
          sm => AvroSchemaUtils.parse(sm.getSchema).equals(schema)
        }

    maybeIdenticalSchemaMetadata.map(_.getId)
  }

  def getIfExistsOrElseRegisterSchema(schema: Schema, subject: SchemaSubject): Int = {
    val maybeSchemaId = findEquivalentSchema(schema, subject)
    maybeSchemaId.getOrElse(register(subject, schema))
  }
}
