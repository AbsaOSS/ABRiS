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

import org.scalatest.{BeforeAndAfter, FlatSpec}
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.schemas.RegistryConfig

class RegistryConfigSpec extends FlatSpec with BeforeAndAfter {

  private val schema = AvroSchemaUtils.parse(
    """{
      |"type": "record",
      |"name": "Blah",
      |"namespace" : "Bleh",
      |"fields": [{ "name": "name", "type": "string" }]
      |}""".stripMargin)

  behavior of "RegistryConfig"

  it should "retrieve the correct subject name for TopicName strategy" in {
    val confValue = Map(
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "foo_topic"
    )

    assert("foo_topic-value" == new RegistryConfig(confValue).subjectName())

    val confKey = Map(
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "foo_topic"
    )

    assert("foo_topic-key" == new RegistryConfig(confKey).subjectName())
  }

  it should "retrieve the correct subject name for RecordName strategy" in {
    val confValue = Map(
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.RECORD_NAME,
      SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "foo_name",
      SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "foo_namespace",
    )

    assert("foo_namespace.foo_name" == new RegistryConfig(confValue).subjectName())

    val confKey = Map(
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.RECORD_NAME,
      SchemaManager.PARAM_KEY_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "foo_name",
      SchemaManager.PARAM_KEY_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "foo_namespace",
    )

    assert("foo_namespace.foo_name" == new RegistryConfig(confKey).subjectName())
  }

  it should "retrieve the correct subject name for TopicRecordName strategy" in {
    val confValue = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "foo_topic",
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME,
      SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "foo_name",
      SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "foo_namespace",
    )

    assert("foo_topic-foo_namespace.foo_name" == new RegistryConfig(confValue).subjectName())

    val confKey = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "foo_topic",
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME,
      SchemaManager.PARAM_KEY_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "foo_name",
      SchemaManager.PARAM_KEY_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "foo_namespace",
    )

    assert("foo_topic-foo_namespace.foo_name" == new RegistryConfig(confKey).subjectName())

    println(schema.getName())
  }

  it should "retrieve name and namespace for TopicRecordName strategy from schema" in {
    val confValue = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "foo_topic",
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME,
    )

    assert("foo_topic-Bleh.Blah" == new RegistryConfig(confValue).subjectName(schema))
  }

  it should "throw InvalidParameterException if both or none value and key naming strategies are set" in {
    val conf = Map(
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME,
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME
    )

    assertThrows[InvalidParameterException] {new RegistryConfig(conf).subjectName()}
    assertThrows[InvalidParameterException] {new RegistryConfig(Map.empty).subjectName()}
  }

  it should "return key id if key naming strategy is set" in {
    val conf = Map(
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME,
      SchemaManager.PARAM_KEY_SCHEMA_ID -> "42",
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> "666"
    )

    assert("42" == new RegistryConfig(conf).schemaIdOption.get)
  }

  it should "return value id if value naming strategy is set" in {
    val conf = Map(
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME,
      SchemaManager.PARAM_KEY_SCHEMA_ID -> "42",
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> "666"
    )

    assert("666" == new RegistryConfig(conf).schemaIdOption.get)
  }
}
