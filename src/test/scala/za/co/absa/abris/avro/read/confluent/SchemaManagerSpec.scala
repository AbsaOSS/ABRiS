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

import io.confluent.common.config.ConfigException
import org.scalatest.{BeforeAndAfter, FlatSpec}
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils

class SchemaManagerSpec extends FlatSpec with BeforeAndAfter {

  private val schema = AvroSchemaUtils.parse(
    "{\"type\": \"record\", \"name\": \"Blah\", \"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}")

  behavior of "SchemaManager"

  before {
    SchemaManager.reset()
    assertResult(false) {SchemaManager.isSchemaRegistryConfigured}
  }

  it should "throw if no strategy is specified" in {
    val topic = "a_subject"
    val conf = Map[String,String]()
    val message1 = intercept[IllegalArgumentException] {
      SchemaManager.getSubjectName(topic, isKey = false, (null, null), conf)
    }
    val message2 = intercept[IllegalArgumentException] {
      SchemaManager.getSubjectName(topic, isKey = true, (null, null), conf)
    }

    assert(message1.getMessage.contains("not specified"))
    assert(message2.getMessage.contains("not specified"))
  }

  it should "retrieve the correct subject name for TopicName strategy" in {
    val subject = "a_subject"
    val conf = Map(
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME,
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_NAME
    )
    assert(subject + "-value" == SchemaManager.getSubjectName(subject, isKey = false, (null, null), conf))
    assert(subject + "-key" == SchemaManager.getSubjectName(subject, isKey = true, (null, null), conf))
  }

  it should "retrieve the correct subject name for RecordName strategy" in {
    val subject = "a_subject"
    val conf = Map(
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.RECORD_NAME,
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.RECORD_NAME
    )

    val schemaName = "schema_name"
    val schemaNamespace = "schema_namespace"

    assert(s"$schemaNamespace.$schemaName" == SchemaManager.getSubjectName(
      subject, isKey = false, (schemaName, schemaNamespace), conf))

    assert(s"$schemaNamespace.$schemaName" == SchemaManager.getSubjectName(
      subject, isKey = true, (schemaName, schemaNamespace), conf))
  }

  it should "throw SchemaManagerException when getting RecordName strategy if schema is null" in {
    val subject = "a_subject"
    val conf = Map(
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.RECORD_NAME,
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.RECORD_NAME
    )

    val schemaName = null
    val schemaNamespace = "namespace"

    assertThrows[SchemaManagerException](
      SchemaManager.getSubjectName(subject, isKey = false, (schemaName, schemaNamespace), conf).isEmpty)

    assertThrows[SchemaManagerException](
      SchemaManager.getSubjectName(subject, isKey = true, (schemaName, schemaNamespace), conf).isEmpty)
  }

  it should "retrieve the correct subject name for TopicRecordName strategy" in {
    val topic = "a_subject"
    val conf = Map(
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME,
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME
    )

    val schemaName = "schema_name"
    val schemaNamespace = "schema_namespace"

    assert(s"$topic-$schemaNamespace.$schemaName" == SchemaManager.getSubjectName(
      topic, isKey = false, (schemaName, schemaNamespace), conf))

    assert(s"$topic-$schemaNamespace.$schemaName" == SchemaManager.getSubjectName(
      topic, isKey = true, (schemaName, schemaNamespace), conf))
  }

  it should "throw when getting TopicRecordName strategy if schema is null" in {
    val subject = "a_subject"
    val conf = Map(
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME,
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME
    )

    val schemaName = null
    val schemaNamespace = "namespace"

    assertThrows[SchemaManagerException](
      SchemaManager.getSubjectName(subject, isKey = false, (schemaName, schemaNamespace), conf).isEmpty)

    assertThrows[SchemaManagerException](
      SchemaManager.getSubjectName(subject, isKey = true, (schemaName, schemaNamespace), conf).isEmpty)
  }

  it should "not try to configure Schema Registry client if parameters are empty" in {
    SchemaManager.configureSchemaRegistry(Map[String,String]())
    assertResult(false) {SchemaManager.isSchemaRegistryConfigured} // should still be unconfigured
  }

  it should "throw when getting subject and id if Schema Registry client is not configured" in {
    assertThrows[SchemaManagerException] {SchemaManager.getBySubjectAndId("subject", 1)}
  }

  it should "throw when getting latest id if Schema Registry client is not configured" in {
    assertThrows[SchemaManagerException] {SchemaManager.getLatestVersionId("subject")}
  }

  it should "throw when registering schema if Schema Registry client is not configured" in {
    assertThrows[SchemaManagerException] {SchemaManager.register(schema, "subject")}
  }

  it should "throw IllegalArgumentException if cluster address is empty or null" in {
    val config1 = Map(SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "")
    val config2 = Map(SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> null)

    assertThrows[IllegalArgumentException] {SchemaManager.configureSchemaRegistry(config1)}
    assertThrows[ConfigException] {SchemaManager.configureSchemaRegistry(config2)}
  }
}
