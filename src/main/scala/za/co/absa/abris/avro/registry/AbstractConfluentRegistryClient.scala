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

import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.{SchemaMetadata, SchemaRegistryClient}
import org.apache.avro.Schema

import java.util


abstract class AbstractConfluentRegistryClient(client: SchemaRegistryClient) extends AbrisRegistryClient {

  override def getAllVersions(subject: String): util.List[Integer] =
    client.getAllVersions(subject)

  override def testCompatibility(subject: String, schema: Schema): Boolean =
    client.testCompatibility(subject, new AvroSchema(schema))

  override def register(subject: String, schema: Schema): Int =
    client.register(subject, new AvroSchema(schema))

  override def getLatestSchemaMetadata(subject: String): SchemaMetadata =
    client.getLatestSchemaMetadata(subject)

  override def getSchemaMetadata(subject: String, version: Int): SchemaMetadata =
    client.getSchemaMetadata(subject, version)

  override def getById(schemaId: Int): Schema = {
    val parsedSchema = client.getSchemaById(schemaId)
    parsedSchema match {
      case schema: AvroSchema => schema.rawSchema()
      case schema => throw new UnsupportedOperationException(s"Only AvroSchema is supported," +
        s" got schema type ${schema.schemaType()}")
    }
  }
}
