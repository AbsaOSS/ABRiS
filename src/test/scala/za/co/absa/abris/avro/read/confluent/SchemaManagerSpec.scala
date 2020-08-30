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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.scalatest.{BeforeAndAfter, FlatSpec}
import za.co.absa.abris.avro.schemas.RegistryConfig

class schemaManagerSpec extends FlatSpec with BeforeAndAfter {

  private val schema =
    "{\"type\": \"record\", \"name\": \"Blah\", \"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}"


  val recordByteSchema = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "record_name",
     "fields":[
         {"name": "int", "type":  ["int", "null"] }
     ]
  }"""

  val recordEvolvedByteSchema1 = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "record_name",
     "fields":[
         {"name": "int", "type": ["int", "null"] },
         {"name": "favorite_color", "type": "string", "default": "green"}
     ]
  }"""

  val recordEvolvedByteSchema2 = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "record_name",
     "fields":[
         {"name": "int", "type": ["int", "null"] },
         {"name": "favorite_color", "type": "string", "default": "green"},
         {"name": "favorite_badger", "type": "string", "default": "Honey badger"}
     ]
  }"""

  behavior of "SchemaManager"

  it should "throw if no strategy is specified" in {
    val conf = Map[String,String]()
    val schemaManager = new SchemaManager(new RegistryConfig(conf), new MockSchemaRegistryClient())

    intercept[InvalidParameterException] {
      schemaManager.downloadSchema()
    }
    intercept[InvalidParameterException] {
      schemaManager.register(recordEvolvedByteSchema1)
    }
  }

  private val dummySchemaRegistryConfig = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "dummy_topic",
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "dummy",
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> "topic.name"
  )


  private val schemaRegistryConfig = Map(
    SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "test_topic",
    SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "dummy",
    SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> "topic.name"
  )

  it should "resolve schema id integer from schema and version" in {

    val client = new MockSchemaRegistryClient()

    val dummySchemaManager = new SchemaManager(new RegistryConfig(dummySchemaRegistryConfig), client)
    // register dummy schema so that ids and versions are different for the rest
    dummySchemaManager.register(schema)                 // id 1, version 1


    val regConfig = new RegistryConfig(schemaRegistryConfig)
    val schemaManager1 = new SchemaManager(regConfig, client)
    // now register several schemas for different topic to create more versions
    schemaManager1.register(recordByteSchema)           // id 2, version 1
    schemaManager1.register(recordEvolvedByteSchema1)   // id 3, version 2
    schemaManager1.register(recordEvolvedByteSchema2)   // id 4, version 3


    val schemaManager2 = new SchemaManager(new RegistryConfig(
      schemaRegistryConfig ++ Map(
        SchemaManager.PARAM_VALUE_SCHEMA_ID -> "4",
        SchemaManager.PARAM_VALUE_SCHEMA_VERSION -> "2"
      )
    ), client)

    assert(schemaManager2.schemaId.get == 4)


    val schemaManager3 = new SchemaManager(new RegistryConfig(
      schemaRegistryConfig ++ Map(
        SchemaManager.PARAM_VALUE_SCHEMA_VERSION -> "2"
      )
    ), client)

    assert(schemaManager3.schemaId.get == 3)


    val schemaManager4 = new SchemaManager(new RegistryConfig(
      schemaRegistryConfig ++ Map(
        SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest"
      )
    ), client)

    assert(schemaManager4.schemaId.get == 4)


    val schemaManager5 = new SchemaManager(new RegistryConfig(
      schemaRegistryConfig ++ Map(
        SchemaManager.PARAM_VALUE_SCHEMA_VERSION -> "latest"
      )
    ), client)

    assert(schemaManager5.schemaId.get == 4)


    val schemaManager6 = new SchemaManager(new RegistryConfig(
      dummySchemaRegistryConfig ++ Map(
        SchemaManager.PARAM_VALUE_SCHEMA_VERSION -> "latest"
      )
    ), client)

    assert(schemaManager6.schemaId.get == 1)

    val schemaManager7 = new SchemaManager(new RegistryConfig(dummySchemaRegistryConfig), client)
    assert(schemaManager7.schemaId == None)
  }


}
