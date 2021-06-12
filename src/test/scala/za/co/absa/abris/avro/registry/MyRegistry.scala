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

import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
import org.apache.avro.Schema

import java.util
import java.util.Optional

/**
 * This is a dummy registry that does nothing.
 */
class MyRegistry(configs: util.Map[String, String]) extends CustomRegistryClient(configs) {

  override def register(s: String, schema: Schema): Int = ???

  override def register(s: String, schema: Schema, i: Int, i1: Int): Int = ???

  override def getByID(i: Int): Schema = ???

  override def getById(i: Int): Schema = ???

  override def getBySubjectAndID(s: String, i: Int): Schema = ???

  override def getBySubjectAndId(s: String, i: Int): Schema = ???

  override def getLatestSchemaMetadata(s: String): SchemaMetadata = ???

  override def getSchemaMetadata(s: String, i: Int): SchemaMetadata = ???

  override def getVersion(s: String, schema: Schema): Int = ???

  override def getAllVersions(s: String): util.List[Integer] = ???

  override def testCompatibility(s: String, schema: Schema): Boolean = ???

  override def updateCompatibility(s: String, s1: String): String = ???

  override def getCompatibility(s: String): String = ???

  override def setMode(s: String): String = ???

  override def setMode(s: String, s1: String): String = ???

  override def getMode: String = ???

  override def getMode(s: String): String = ???

  override def getAllSubjects: util.Collection[String] = ???

  override def getId(s: String, schema: Schema): Int = ???

  override def deleteSubject(s: String): util.List[Integer] = ???

  override def deleteSubject(map: util.Map[String, String], s: String): util.List[Integer] = ???

  override def deleteSchemaVersion(s: String, s1: String): Integer = ???

  override def deleteSchemaVersion(map: util.Map[String, String], s: String, s1: String): Integer = ???

  override def reset(): Unit = ???

  override def parseSchema(schemaType: String, schemaString: String, references: util.List[SchemaReference]): Optional[ParsedSchema] = ???

  override def register(subject: String, schema: ParsedSchema): Int = ???

  override def register(subject: String, schema: ParsedSchema, version: Int, id: Int): Int = ???

  override def getSchemaById(id: Int): ParsedSchema = ???

  override def getSchemaBySubjectAndId(subject: String, id: Int): ParsedSchema = ???

  override def getAllSubjectsById(id: Int): util.Collection[String] = ???

  override def getVersion(subject: String, schema: ParsedSchema): Int = ???

  override def testCompatibility(subject: String, schema: ParsedSchema): Boolean = ???

  override def getId(subject: String, schema: ParsedSchema): Int = ???
}
