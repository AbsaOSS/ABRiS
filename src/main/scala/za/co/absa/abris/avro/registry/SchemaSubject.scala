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

import io.confluent.kafka.serializers.subject.{RecordNameStrategy, TopicNameStrategy, TopicRecordNameStrategy}
import org.apache.avro.Schema

/**
 * Represents Confluent Schema Registry Subject created using naming strategy
 *
 * https://docs.confluent.io/current/schema-registry/serdes-develop/index.html#how-the-naming-strategies-work
 *
 */
class SchemaSubject(val asString: String)

object SchemaSubject{
  private val TOPIC_NAME_STRATEGY = new TopicNameStrategy()
  private val RECORD_NAME_STRATEGY = new RecordNameStrategy()
  private val TOPIC_RECORD_NAME_STRATEGY = new TopicRecordNameStrategy()

  private val dummySchema = Schema.createRecord("name", "", "namespace", false)

  def usingTopicNameStrategy(
    topicName: String,
    isKey: Boolean = false
  ): SchemaSubject = {
    new SchemaSubject(TOPIC_NAME_STRATEGY.subjectName(topicName, isKey, dummySchema))
  }

  def usingRecordNameStrategy(
    recordName: String,
    recordNamespace: String,
  ): SchemaSubject = {
    val dummySchema = Schema.createRecord(recordName, "", recordNamespace, false)
    new SchemaSubject(RECORD_NAME_STRATEGY.subjectName("", false, dummySchema))
  }

  def usingRecordNameStrategy(
    schema: Schema,
  ): SchemaSubject = {
    new SchemaSubject(RECORD_NAME_STRATEGY.subjectName("", false, schema))
  }

  def usingTopicRecordNameStrategy(
    topicName: String,
    recordName: String,
    recordNamespace: String,
  ): SchemaSubject = {
    val dummySchema = Schema.createRecord(recordName, "", recordNamespace, false)
    new SchemaSubject(TOPIC_RECORD_NAME_STRATEGY.subjectName(topicName, false, dummySchema))
  }

  def usingTopicRecordNameStrategy(
    topicName: String,
    schema: Schema,
  ): SchemaSubject = {
    new SchemaSubject(TOPIC_RECORD_NAME_STRATEGY.subjectName(topicName, false, schema))
  }
}
