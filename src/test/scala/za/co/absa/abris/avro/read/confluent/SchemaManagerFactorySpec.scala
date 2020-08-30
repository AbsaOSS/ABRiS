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

import io.confluent.kafka.schemaregistry.client.{MockSchemaRegistryClient, SchemaRegistryClient}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, FlatSpec, PrivateMethodTester}
import za.co.absa.abris.avro.schemas.RegistryConfig

class SchemaManagerFactorySpec extends FlatSpec with BeforeAndAfterEach with PrivateMethodTester {

  val test = PrivateMethod[SchemaRegistryClient]('schemaRegistryClient_)

  private val schemaRegistryConfig1 = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "test_topic",
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "http://dummy",
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> "topic.record.name",
    SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "native_complete",
    SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "all-types.test"
  )

  private val schemaRegistryConfig2 = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "test_topic",
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "http://dummy_sr_2",
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> "topic.record.name",
    SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "native_complete",
    SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "all-types.test"
  )

  override def beforeEach(): Unit = {
    super.beforeEach()
    SchemaManagerFactory.resetSRClientInstance() // Reset factory state
  }

  behavior of "SchemaManagerFactory"

  it should "create a schema manager for the given Schema Registry configs " +
            "and cache the Schema Registry Client reference for subsequent usages" in {
    val schemaManagerRef1 = SchemaManagerFactory.create(schemaRegistryConfig1)
    val schemaManagerRef2 = SchemaManagerFactory.create(schemaRegistryConfig1)

    val field = schemaManagerRef1.getClass.getDeclaredField("schemaRegistryClient")
    field.setAccessible(true)

    val res1 = field.get(schemaManagerRef1).asInstanceOf[SchemaRegistryClient]
    val res2 = field.get(schemaManagerRef2).asInstanceOf[SchemaRegistryClient]
    assert(res1.eq(res2))
  }

  it should "create a schema manager with a different schema registry client depending on the configs passed" in {
    val schemaManagerRef1 = SchemaManagerFactory.create(schemaRegistryConfig1)
    val schemaManagerRef2 = SchemaManagerFactory.create(schemaRegistryConfig2)

    val field = schemaManagerRef1.getClass.getDeclaredField("schemaRegistryClient")
    field.setAccessible(true)

    val res1 = field.get(schemaManagerRef1).asInstanceOf[SchemaRegistryClient]
    val res2 = field.get(schemaManagerRef2).asInstanceOf[SchemaRegistryClient]
    assert(!res1.eq(res2))
  }

}
