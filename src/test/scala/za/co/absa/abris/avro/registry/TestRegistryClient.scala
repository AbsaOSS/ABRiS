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
import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import org.apache.avro.Schema

import java.util

class TestRegistryClient(config: Map[String, String]) extends AbrisRegistryClient {

  override def getAllVersions(subject: String): util.List[Integer] = ???

  override def testCompatibility(subject: String, schema: Schema): Boolean = ???

  override def register(subject: String, schema: Schema): Int = ???

  override def getLatestSchemaMetadata(subject: String): SchemaMetadata = ???

  override def getSchemaMetadata(subject: String, version: Int): SchemaMetadata = ???

  override def getById(schemaId: Int): Schema = ???
}
