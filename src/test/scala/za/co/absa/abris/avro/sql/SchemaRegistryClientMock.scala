/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.abris.avro.sql

import java.util

import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import org.apache.avro.Schema

/**
 * simple mock that just remembers the last registered schema
 */
class SchemaRegistryClientMock extends SchemaRegistryClient {

  private var cachedSchema: Option[Schema] = None
  private val schemaId = 42

  def reset(): Unit = { cachedSchema = None }

  override def register(s: String, schema: Schema): Int = {
    cachedSchema = Some(schema)
    schemaId
  }

  override def getByID(i: Int): Schema = cachedSchema.get

  override def getById(i: Int): Schema = cachedSchema.get

  override def getBySubjectAndID(s: String, i: Int): Schema = cachedSchema.get

  override def getBySubjectAndId(s: String, i: Int): Schema = cachedSchema.get

  override def getLatestSchemaMetadata(s: String): SchemaMetadata = cachedSchema match {
    case Some(schema) => new SchemaMetadata(schemaId, 1, schema.toString(true))
    case None         => throw new RuntimeException("Subject not found")
  }

  override def getSchemaMetadata(s: String, i: Int): SchemaMetadata = getLatestSchemaMetadata(s)

  override def getVersion(s: String, schema: Schema): Int = 1

  override def getAllVersions(s: String): util.List[Integer] = {
    throw new UnsupportedOperationException()
  }
  override def testCompatibility(s: String, schema: Schema): Boolean = true

  override def updateCompatibility(s: String, s1: String): String = {
    throw new UnsupportedOperationException()
  }

  override def getCompatibility(s: String): String = {
    throw new UnsupportedOperationException()
  }

  override def getAllSubjects: util.Collection[String] = {
    throw new UnsupportedOperationException()
  }

  override def getId(s: String, schema: Schema): Int = {
    throw new UnsupportedOperationException()
  }

  override def deleteSubject(s: String): util.List[Integer] = {
    throw new UnsupportedOperationException()
  }

  override def deleteSubject(map: util.Map[String, String], s: String): util.List[Integer] = {
    throw new UnsupportedOperationException()
  }

  override def deleteSchemaVersion(s: String, s1: String): Integer = {
    throw new UnsupportedOperationException()
  }

  override def deleteSchemaVersion(map: util.Map[String, String], s: String, s1: String): Integer = {
    throw new UnsupportedOperationException()
  }
}
