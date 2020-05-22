/*
 * Copyright 2020 ABSA Group Limited
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

package za.co.absa.abris.avro.schemas

import java.security.InvalidParameterException

import org.apache.avro.Schema
import za.co.absa.abris.avro.read.confluent.SchemaManager._
import za.co.absa.abris.avro.read.confluent.{SchemaManager, SchemaManagerException}
import za.co.absa.abris.avro.subject.SubjectNameStrategyAdapterFactory

/**
 * This class wraps Abris configuration and provide methods to acces it easily.
 *
 * @param params configuration
 */
class RegistryConfig(params: Map[String,String]) {

  private lazy val isKey: Boolean = {
    val valueStrategy = params.get(SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY)
    val keyStrategy = params.get(SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY)

    (valueStrategy, keyStrategy) match {
      case (Some(valueStrategy), None) => false
      case (None, Some(keyStrategy)) => true
      case (Some(_), Some(_)) =>
        throw new InvalidParameterException(
          "Both key.schema.naming.strategy and value.schema.naming.strategy were defined. " +
            "Only one of them supoused to be defined!")
      case _ =>
        throw new InvalidParameterException(
          "At least one of key.schema.naming.strategy or value.schema.naming.strategy " +
            "must be defined to use schema registry!")
    }
  }

  private def registryTopicOption = params.get(SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC)

  private def schemaNamingStrategy(): String =
    params(if (isKey) PARAM_KEY_SCHEMA_NAMING_STRATEGY else PARAM_VALUE_SCHEMA_NAMING_STRATEGY)

  def schemaIdOption: Option[String] =
    params.get(if (isKey) PARAM_KEY_SCHEMA_ID else PARAM_VALUE_SCHEMA_ID)

  def schemaVersionOption: Option[String] =
    params.get(if (isKey) PARAM_KEY_SCHEMA_VERSION else PARAM_VALUE_SCHEMA_VERSION)

  private def schemaNameOption: Option[String] =
    params.get(if (isKey) PARAM_KEY_SCHEMA_NAME_FOR_RECORD_STRATEGY else PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY)

  private def schemaNameSpaceOption: Option[String] = params.get(
    if (isKey) PARAM_KEY_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY else PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY)

  def subjectName(): String = {
    val name = schemaNameOption.getOrElse(null)
    val namespace = schemaNameSpaceOption.getOrElse(null)
    subjectName(Schema.createRecord(name, "", namespace, false))
  }

  def subjectName(schema: Schema): String = {
    val adapter = SubjectNameStrategyAdapterFactory.build(schemaNamingStrategy())

    if (adapter.validate(schema)) {
      adapter.subjectName(registryTopicOption.getOrElse(null), isKey, schema)
    }
    else {
      throw new SchemaManagerException(
        s"Invalid configuration for naming strategy. Are you using RecordName or TopicRecordName? " +
          s"If yes, are you providing SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY and " +
          s"SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY in the configuration map?")
    }
  }

  def isIdOrVersionDefined: Boolean = schemaIdOption.orElse(schemaVersionOption).isDefined
}
