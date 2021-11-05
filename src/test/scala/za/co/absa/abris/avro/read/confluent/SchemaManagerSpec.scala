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

import org.scalatest.{BeforeAndAfter, FlatSpec}
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.registry.{ConfluentMockRegistryClient, LatestVersion, NumVersion, SchemaSubject}
import za.co.absa.abris.config.AbrisConfig

class schemaManagerSpec extends FlatSpec with BeforeAndAfter {

  private val schema = AvroSchemaUtils.parse(
    "{\"type\": \"record\", \"name\": \"Blah\", \"fields\": [{ \"name\": \"name\", \"type\": \"string\" }]}")


  val recordByteSchema = AvroSchemaUtils.parse("""{
     "namespace": "all-types.test",
     "type": "record",
     "name": "record_name",
     "fields":[
         {"name": "int", "type":  ["int", "null"] }
     ]
  }""")

  val recordEvolvedByteSchema1 = AvroSchemaUtils.parse("""{
     "namespace": "all-types.test",
     "type": "record",
     "name": "record_name",
     "fields":[
         {"name": "int", "type": ["int", "null"] },
         {"name": "favorite_color", "type": "string", "default": "green"}
     ]
  }""")

  val recordEvolvedByteSchema2 = AvroSchemaUtils.parse("""{
     "namespace": "all-types.test",
     "type": "record",
     "name": "record_name",
     "fields":[
         {"name": "int", "type": ["int", "null"] },
         {"name": "favorite_color", "type": "string", "default": "green"},
         {"name": "favorite_badger", "type": "string", "default": "Honey badger"}
     ]
  }""")

  val registryUrl = "dummyUrl"
  val registryConfig = Map(AbrisConfig.SCHEMA_REGISTRY_URL -> registryUrl)

  behavior of "SchemaManager"

  it should "return correct schema by id or subect and version" in {
    val schemaManager = new SchemaManager(new ConfluentMockRegistryClient())
    val subject1 = SchemaSubject.usingTopicNameStrategy("foo")
    val subject2 = SchemaSubject.usingTopicNameStrategy("bar")

    val id1 = schemaManager.register(subject1, schema)                     // id1, version 1
    val id2 = schemaManager.register(subject2, recordByteSchema)           // id2, version 1
    val id3 = schemaManager.register(subject2, recordEvolvedByteSchema1)   // id3, version 2
    val id4 = schemaManager.register(subject2, recordEvolvedByteSchema2)   // id4, version 3

    assert(schemaManager.getSchemaById(id1) == schema)
    assert(schemaManager.getSchemaById(id2) == recordByteSchema)
    assert(schemaManager.getSchemaById(id3) == recordEvolvedByteSchema1)
    assert(schemaManager.getSchemaById(id4) == recordEvolvedByteSchema2)

    assert(schemaManager.getSchemaBySubjectAndVersion(subject1, NumVersion(1)) == schema)
    assert(schemaManager.getSchemaBySubjectAndVersion(subject1, LatestVersion()) == schema)

    assert(schemaManager.getSchemaBySubjectAndVersion(subject2, NumVersion(1)) == recordByteSchema)
    assert(schemaManager.getSchemaBySubjectAndVersion(subject2, NumVersion(2)) == recordEvolvedByteSchema1)
    assert(schemaManager.getSchemaBySubjectAndVersion(subject2, NumVersion(3)) == recordEvolvedByteSchema2)
    assert(schemaManager.getSchemaBySubjectAndVersion(subject2, LatestVersion()) == recordEvolvedByteSchema2)
  }

  it should "find already existing schema" in {
    val schemaManager = new SchemaManager(new ConfluentMockRegistryClient())

    val subject = SchemaSubject.usingTopicNameStrategy("dummy_topic")

    schemaManager.register(subject, recordByteSchema)
    schemaManager.register(subject, recordEvolvedByteSchema1)
    schemaManager.register(subject, recordEvolvedByteSchema2)

    val maybeId = schemaManager.findEquivalentSchema(recordEvolvedByteSchema1, subject)

    val resultSchema = schemaManager.getSchemaById(maybeId.get)

    assert(resultSchema.equals(recordEvolvedByteSchema1))
  }

  "exists" should "return true when schema is in registry" in {
    val schemaManager = new SchemaManager(new ConfluentMockRegistryClient())

    val subject = SchemaSubject.usingTopicNameStrategy("dummy_topic")
    schemaManager.register(subject, recordByteSchema)
    val schemaExists = schemaManager.exists(subject)

    assert(schemaExists == true)
  }

  "exists" should "return false when schema is not in registry" in {
    val schemaManager = new SchemaManager(new ConfluentMockRegistryClient())
    val schemaExists = schemaManager.exists(SchemaSubject.usingTopicNameStrategy("foo"))

    assert(schemaExists == false)
  }
}
