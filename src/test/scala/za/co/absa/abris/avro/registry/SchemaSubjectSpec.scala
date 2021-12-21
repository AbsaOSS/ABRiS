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

package za.co.absa.abris.avro.registry

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils

class SchemaSubjectSpec extends AnyFlatSpec with BeforeAndAfter {

  private val schema = AvroSchemaUtils.parse(
    """{
      |"type": "record",
      |"name": "Blah",
      |"namespace" : "Bleh",
      |"fields": [{ "name": "name", "type": "string" }]
      |}""".stripMargin)

  behavior of "SchemaSubject"

  it should "retrieve the correct subject name for TopicName strategy" in {

    assertResult("foo_topic-value")(
      SchemaSubject.usingTopicNameStrategy("foo_topic").asString
    )

    assertResult("foo_topic-key")(
      SchemaSubject.usingTopicNameStrategy("foo_topic", isKey = true).asString
    )
  }

  it should "retrieve the correct subject name for RecordName strategy" in {

    assertResult("foo_namespace.foo_name")(
      SchemaSubject.usingRecordNameStrategy("foo_name", "foo_namespace").asString
    )
  }

  it should "retrieve the correct subject name for TopicRecordName strategy" in {

    assertResult("topic-foo_namespace.foo_name")(
      SchemaSubject.usingTopicRecordNameStrategy("topic", "foo_name", "foo_namespace").asString
    )
  }

  it should "retrieve name and namespace for RecordName strategy from schema" in {

    assertResult("Bleh.Blah")(
      SchemaSubject.usingRecordNameStrategy(schema).asString
    )
  }

  it should "retrieve name and namespace for TopicRecordName strategy from schema" in {

    assertResult("foo_topic-Bleh.Blah")(
      SchemaSubject.usingTopicRecordNameStrategy("foo_topic", schema).asString
    )
  }
}
