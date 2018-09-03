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

import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.schemas.SchemaLoader

/**
 * This class provides utility methods to cope with Avro schemas.
 */
object AvroSchemaUtils {

  private val logger = LoggerFactory.getLogger(AvroSchemaUtils.getClass)

  private def configureSchemaManager(schemaRegistryConf: Map[String,String]) = {
    if (!SchemaManager.isSchemaRegistryConfigured()) {
      SchemaManager.configureSchemaRegistry(schemaRegistryConf)
    }
  }

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

  def load(schemaRegistryConf: Map[String,String]): Schema = {
    SchemaLoader.loadFromSchemaRegistry(schemaRegistryConf)
  }

  def loadForKeyAndValue(schemaRegistryConf: Map[String,String]): (Schema,Schema) = {
    SchemaLoader.loadFromSchemaRegistryForKeyAndValue(schemaRegistryConf)
  }

  /**
    * Register a new schema for a subject KEY if the schema is compatible with the latest available version.
    *
    * @return None if incompatible or if could not perform the registration.
    */
  def registerIfCompatibleKeySchema(topic: String, schema: Schema, schemaRegistryConf: Map[String,String]): Option[Int] = {
    registerIfCompatibleSchema(topic, schema, schemaRegistryConf, true)
  }

  /**
    * Register a new schema for a subject VALUE if the schema is compatible with the latest available version.
    *
    * @return None if incompatible or if could not perform the registration.
    */
  def registerIfCompatibleValueSchema(topic: String, schema: Schema, schemaRegistryConf: Map[String,String]): Option[Int] = {
    registerIfCompatibleSchema(topic, schema, schemaRegistryConf, false)
  }

  /**
    * Register a new schema for a subject if the schema is compatible with the latest available version.
    *
    * @return None if incompatible or if could not perform the registration.
    */
  private def registerIfCompatibleSchema(topic: String, schema: Schema, schemaRegistryConf: Map[String,String], isKey: Boolean): Option[Int] = {

    configureSchemaManager(schemaRegistryConf)

    val subject = SchemaManager.getSubjectName(topic, isKey)
    if (!SchemaManager.exists(subject) || SchemaManager.isCompatible(schema, subject)) {
      logger.info(s"AvroSchemaUtils.registerIfCompatibleSchema: Registering schema for subject: $subject")
      SchemaManager.register(schema, subject)
    }
    else {
      logger.error(s"Schema incompatible with latest for subject '$subject' in Schema Registry")
      None
    }
  }

  /**
   * Loads an Avro's plain schema from the path.
   */
  def loadPlain(path: String) = {
    SchemaLoader.loadFromFile(path)
  }
}