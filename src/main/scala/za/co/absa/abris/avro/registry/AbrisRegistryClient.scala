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

trait AbrisRegistryClient {

  def getAllVersions(subject: String): java.util.List[Integer]

  def testCompatibility(subject: String, schema: Schema): Boolean

  def register(subject: String, schema: Schema): Int

  def getLatestSchemaMetadata(subject: String): SchemaMetadata

  def getSchemaMetadata(subject: String, version: Int): SchemaMetadata

  def getById(schemaId: Int): Schema

}
