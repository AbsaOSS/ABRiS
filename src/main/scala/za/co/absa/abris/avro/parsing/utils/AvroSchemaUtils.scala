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

package za.co.absa.abris.avro.parsing.utils

import java.security.InvalidParameterException

import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.schemas.SchemaLoader

/**
 * This class provides utility methods to cope with Avro schemas.
 */
object AvroSchemaUtils {

  private val logger = LoggerFactory.getLogger(AvroSchemaUtils.getClass)

  /**
   * Parses a plain Avro schema into an org.apache.avro.Schema implementation.
   */
  def parse(schema: String): Schema = new Schema.Parser().parse(schema)

  /**
   * Loads an Avro org.apache.avro.Schema from the path.
   */
  def load(path: String): Schema = {
    parse(loadPlain(path))
  }

  def loadForValue(schemaRegistryConf: Map[String,String]): Schema = {
    SchemaLoader.loadFromSchemaRegistryValue(schemaRegistryConf)
  }

  def loadForKey(schemaRegistryConf: Map[String,String]): Schema = {
    SchemaLoader.loadFromSchemaRegistryKey(schemaRegistryConf)
  }

  def load(schemaVersion: Int, schemaRegistryConf: Map[String,String]): SchemaMetadata = {
    SchemaLoader.loadFromSchemaRegistry(schemaVersion, schemaRegistryConf)
  }

  /**
    * Register a new schema for a subject KEY if the schema is compatible with the latest available version.
    *
    * @return None if incompatible or if could not perform the registration.
    */
  def registerIfCompatibleKeySchema(topic: String, schema: Schema, schemaRegistryConf: Map[String,String]):
  Option[Int] = {
    registerIfCompatibleSchema(topic, schema, schemaRegistryConf, isKey = true)
  }

  /**
    * Register a new schema for a subject VALUE if the schema is compatible with the latest available version.
    *
    * @return None if incompatible or if could not perform the registration.
    */
  def registerIfCompatibleValueSchema(
    topic: String,
    schema: Schema,
    schemaRegistryConf: Map[String,String]): Option[Int] = {

    registerIfCompatibleSchema(topic, schema, schemaRegistryConf, isKey = false)
  }

  /**
    * Register a new schema for a subject if the schema is compatible with the latest available version.
    *
    * @return None if incompatible or if could not perform the registration.
    */
  private def registerIfCompatibleSchema(
    topic: String,
    schema: Schema,
    schemaRegistryConf: Map[String,String],
    isKey: Boolean): Option[Int] = {

    SchemaManager.configure(schemaRegistryConf)

    val subject = SchemaManager.getSubjectName(topic, isKey, schema, schemaRegistryConf)

    if (!SchemaManager.exists(subject) || SchemaManager.isCompatible(schema, subject)) {
      logger.info(s"AvroSchemaUtils.registerIfCompatibleSchema: Registering schema for subject: $subject")
      Some(SchemaManager.register(schema, subject))
    }
    else {
      logger.error(s"Schema incompatible with latest for subject '$subject' in Schema Registry")
      None
    }
  }

  /**
   * Loads an Avro's plain schema from the path.
   */
  def loadPlain(path: String): String = {
    SchemaLoader.loadFromFile(path)
  }

  /**
   * Tries to manage schema registration in case credentials to access Schema Registry are provided.
   */
  @throws[InvalidParameterException]
  def registerSchema(schemaAsString: String, registryConfig: Map[String,String]): Option[Int] =
    registerSchema(parse(schemaAsString), registryConfig)

  /**
   * Tries to manage schema registration in case credentials to access Schema Registry are provided.
   */
  @throws[InvalidParameterException]
  def registerSchema(schema: Schema, registryConfig: Map[String,String]): Option[Int] = {

    val topic = registryConfig(SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC)

    val valueStrategy = registryConfig.get(SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY)
    val keyStrategy = registryConfig.get(SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY)

    val schemaId = (valueStrategy, keyStrategy) match {
      case (Some(valueStrategy), None) => AvroSchemaUtils.registerIfCompatibleValueSchema(topic, schema, registryConfig)
      case (None, Some(keyStrategy)) => AvroSchemaUtils.registerIfCompatibleKeySchema(topic, schema, registryConfig)
      case (Some(_), Some(_)) =>
        throw new InvalidParameterException(
          "Both key.schema.naming.strategy and value.schema.naming.strategy were defined. " +
            "Only one of them supposed to be defined!")
      case _ =>
        throw new InvalidParameterException(
          "At least one of key.schema.naming.strategy or value.schema.naming.strategy " +
            "must be defined to use schema registry!")
    }

    if (schemaId.isEmpty) {
      throw new InvalidParameterException(s"Schema could not be registered for topic '$topic'. " +
        "Make sure that the Schema Registry is available, the parameters are correct and the schemas ar compatible")
    }

    schemaId
  }
}
