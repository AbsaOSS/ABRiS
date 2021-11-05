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
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaMetadata, SchemaRegistryClient}
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.avro.Schema

import java.util
import scala.collection.JavaConverters._

class ConfluentRegistryClient(client: SchemaRegistryClient) extends AbrisRegistryClient {

  def this(configs: Map[String,String]) = this(ConfluentRegistryClient.createClient(configs))

  override def getAllVersions(subject: String): util.List[Integer] =
    client.getAllVersions(subject)

  override def testCompatibility(subject: String, schema: Schema): Boolean =
    client.testCompatibility(subject, schema)

  override def register(subject: String, schema: Schema): Int =
    client.register(subject, schema)

  override def getLatestSchemaMetadata(subject: String): SchemaMetadata =
    client.getLatestSchemaMetadata(subject)

  override def getSchemaMetadata(subject: String, version: Int): SchemaMetadata =
    client.getSchemaMetadata(subject, version)

  override def getById(schemaId: Int): Schema =
    client.getById(schemaId)
}

object ConfluentRegistryClient {

  private def createClient(configs: Map[String,String]) = {
    val settings = new KafkaAvroDeserializerConfig(configs.asJava)
    val urls = settings.getSchemaRegistryUrls
    val maxSchemaObject = settings.getMaxSchemasPerSubject

    new CachedSchemaRegistryClient(urls, maxSchemaObject, configs.asJava)
  }

}
