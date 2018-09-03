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

  private val schema = AvroSchemaUtils.parse("{\"type\": \"record\", \"name\": \"Blah\", \"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}")
  behavior of "SchemaManager"

  before {
    SchemaManager.reset()
    assertResult(false) {SchemaManager.isSchemaRegistryConfigured()}
  }

  it should "retrieve the correct subject name" in {
    val subject = "a_subject"
    assert(subject + "-value" == SchemaManager.getSubjectName(subject, false))
    assert(subject + "-key" == SchemaManager.getSubjectName(subject, true))
  }

  it should "not try to configure Schema Registry client if parameters are empty" in {
    SchemaManager.configureSchemaRegistry(Map[String,String]())
    assertResult(false) {SchemaManager.isSchemaRegistryConfigured()} // should still be unconfigured
  }

  it should "return None as schema if Schema Registry client is not configured" in {
    assertResult(None) {SchemaManager.getBySubjectAndId("subject", 1)}
  }

  it should "return None as latest version if Schema Registry client is not configured" in {
    assertResult(None) {SchemaManager.getLatestVersion("subject")}
  }

  it should "return None as registered schema id if Schema Registry client is not configured" in {
    assertResult(None) {SchemaManager.register(schema, "subject")}
  }

  it should "throw IllegalArgumentException if cluster address is empty or null" in {
    val config1 = Map(SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "")
    val config2 = Map(SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> null)

    assertThrows[IllegalArgumentException] {SchemaManager.configureSchemaRegistry(config1)}
    assertThrows[ConfigException] {SchemaManager.configureSchemaRegistry(config2)}
  }
}