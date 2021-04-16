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

import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
import io.confluent.kafka.serializers.subject.{RecordNameStrategy, TopicNameStrategy, TopicRecordNameStrategy}
import org.apache.avro.Schema

import java.util

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

  private val schemaProvider = new AvroSchemaProvider()
  private val TOPIC_NAME_STRATEGY = new TopicNameStrategy()
  private val RECORD_NAME_STRATEGY = new RecordNameStrategy()
  private val TOPIC_RECORD_NAME_STRATEGY = new TopicRecordNameStrategy()


  def usingTopicNameStrategy(
    topicName: String,
    isKey: Boolean = false
  ): SchemaSubject = {
    val dummySchema = createDummySchema("name", "namespace")
    val parsedSchema = schemaProvider.parseSchema(dummySchema.toString, new util.ArrayList[SchemaReference]())
    new SchemaSubject(TOPIC_NAME_STRATEGY.subjectName(topicName, isKey, parsedSchema.orElseThrow()))
  }

  def usingRecordNameStrategy(
    recordName: String,
    recordNamespace: String
  ): SchemaSubject = {
    val dummySchema = createDummySchema(recordName, recordNamespace)
    val parsedSchema = schemaProvider.parseSchema(dummySchema.toString, new util.ArrayList[SchemaReference]())
    new SchemaSubject(RECORD_NAME_STRATEGY.subjectName("", false, parsedSchema.orElseThrow()))
  }

  def usingRecordNameStrategy(
    schema: Schema
  ): SchemaSubject = {
    val parsedSchema = schemaProvider.parseSchema(schema.toString, new util.ArrayList[SchemaReference]())
    new SchemaSubject(RECORD_NAME_STRATEGY.subjectName("", false, parsedSchema.orElseThrow()))
  }

  def usingTopicRecordNameStrategy(
    topicName: String,
    recordName: String,
    recordNamespace: String
  ): SchemaSubject = {
    val dummySchema = createDummySchema(recordName, recordNamespace)
    val parsedSchema = schemaProvider.parseSchema(dummySchema.toString, new util.ArrayList[SchemaReference]())
    new SchemaSubject(TOPIC_RECORD_NAME_STRATEGY.subjectName(topicName, false, parsedSchema.orElseThrow()))
  }

  def usingTopicRecordNameStrategy(
    topicName: String,
    schema: Schema
  ): SchemaSubject = {
    val parsedSchema = schemaProvider.parseSchema(schema.toString, new util.ArrayList[SchemaReference]())
    new SchemaSubject(TOPIC_RECORD_NAME_STRATEGY.subjectName(topicName, false, parsedSchema.orElseThrow()))
  }

  /*
  io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider	[ScalaTest-run-running-SchemaEvolutionSpec]	Could not parse Avro schema
org.apache.avro.SchemaParseException: Record has no fields: {"type":"record","name":"record_name","namespace":"all-types.test","doc":""}
   */
  private def createDummySchema(name: String, namespace: String) = {
    val rec = Schema.createRecord(name, "helpful documentation", namespace, false)
    val field = new Schema.Field("bla", Schema.create(Schema.Type.INT), "bla", 0)
    rec.setFields(java.util.Arrays.asList(field))
    rec
  }
}
