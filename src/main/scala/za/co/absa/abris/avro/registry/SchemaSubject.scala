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

package za.co.absa.abris.avro.registry

import org.apache.avro.Schema
import org.apache.avro.Schema.Type

/**
 * Represents Confluent Schema Registry Subject created using naming strategy
 *
 * https://docs.confluent.io/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work
 *
 */
class SchemaSubject(val asString: String) {
  override def toString: String = asString
}

object SchemaSubject{

  def usingTopicNameStrategy(
    topicName: String,
    isKey: Boolean = false
  ): SchemaSubject = {
    val suffix = if (isKey) "-key" else "-value"
    new SchemaSubject(topicName + suffix)
  }

  def usingRecordNameStrategy(
    recordName: String,
    recordNamespace: String
  ): SchemaSubject = {
    val dummySchema = createDummySchema(recordName, recordNamespace)
    new SchemaSubject(getRecordName(dummySchema))
  }

  def usingRecordNameStrategy(
    schema: Schema
  ): SchemaSubject = {
    new SchemaSubject(getRecordName(schema))
  }

  def usingTopicRecordNameStrategy(
    topicName: String,
    recordName: String,
    recordNamespace: String
  ): SchemaSubject = {
    val dummySchema = createDummySchema(recordName, recordNamespace)
    new SchemaSubject(topicName + "-" + getRecordName(dummySchema))
  }

  def usingTopicRecordNameStrategy(
    topicName: String,
    schema: Schema
  ): SchemaSubject = {
    new SchemaSubject(topicName + "-" + getRecordName(schema))
  }

  private def getRecordName(schema: Schema): String =
    if (schema.getType == Type.RECORD) {
      schema.getFullName
    } else {
      throw new IllegalArgumentException(s"Schema must be of type RECORD not ${schema.getType}")
    }

  private def createDummySchema(name: String, namespace: String) =
    Schema.createRecord(name, "", namespace, false)
}
