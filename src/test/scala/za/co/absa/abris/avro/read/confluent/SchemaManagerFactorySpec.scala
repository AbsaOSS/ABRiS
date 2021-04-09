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

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import org.scalatest.{BeforeAndAfterEach, FlatSpec}
import za.co.absa.abris.avro.registry.MyRegistry
import za.co.absa.abris.config.AbrisConfig

import scala.reflect.runtime.{universe => ru}

class SchemaManagerFactorySpec extends FlatSpec with BeforeAndAfterEach {

  private val schemaRegistryConfig1 = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> "http://dummy")

  private val schemaRegistryConfig2 = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> "http://dummy_sr_2")

  private val schemaRegistryConfig3 = Map(
    AbrisConfig.SCHEMA_REGISTRY_URL -> "http://dummy_sr_2",
    AbrisConfig.SCHEMA_REGISTRY_CLASS -> "za.co.absa.abris.avro.registry.MyRegistry"
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

    val m = ru.runtimeMirror(schemaManagerRef1.getClass.getClassLoader)
    val fieldTerm = ru.typeOf[SchemaManager].decl(ru.TermName("schemaRegistryClient")).asTerm

    val res1 = m.reflect(schemaManagerRef1).reflectField(fieldTerm).get.asInstanceOf[SchemaRegistryClient]
    val res2 = m.reflect(schemaManagerRef2).reflectField(fieldTerm).get.asInstanceOf[SchemaRegistryClient]
    assert(res1.eq(res2))
  }

  it should "create a schema manager with a different schema registry client depending on the configs passed" in {
    val schemaManagerRef1 = SchemaManagerFactory.create(schemaRegistryConfig1)
    val schemaManagerRef2 = SchemaManagerFactory.create(schemaRegistryConfig2)

    val m = ru.runtimeMirror(schemaManagerRef1.getClass.getClassLoader)
    val fieldTerm = ru.typeOf[SchemaManager].decl(ru.TermName("schemaRegistryClient")).asTerm

    val res1 = m.reflect(schemaManagerRef1).reflectField(fieldTerm).get.asInstanceOf[SchemaRegistryClient]
    val res2 = m.reflect(schemaManagerRef2).reflectField(fieldTerm).get.asInstanceOf[SchemaRegistryClient]
    assert(!res1.eq(res2))
  }

  it should "create a schema manager with a custom schema registry client depending on the configs passed" in {
    val schemaManagerRef1 = SchemaManagerFactory.create(schemaRegistryConfig1)
    val schemaManagerRef3 = SchemaManagerFactory.create(schemaRegistryConfig3)

    val m = ru.runtimeMirror(schemaManagerRef1.getClass.getClassLoader)
    val fieldTerm = ru.typeOf[SchemaManager].decl(ru.TermName("schemaRegistryClient")).asTerm

    val res1 = m.reflect(schemaManagerRef1).reflectField(fieldTerm).get.asInstanceOf[SchemaRegistryClient]
    val res3 = m.reflect(schemaManagerRef3).reflectField(fieldTerm).get.asInstanceOf[SchemaRegistryClient]
    assert(!res1.eq(res3))
    assert(res1.isInstanceOf[CachedSchemaRegistryClient])
    assert(res3.isInstanceOf[MyRegistry])
  }
}
