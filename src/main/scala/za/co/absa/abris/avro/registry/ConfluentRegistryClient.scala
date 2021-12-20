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
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig

import scala.collection.JavaConverters._

class ConfluentRegistryClient(client: SchemaRegistryClient) extends AbstractAbrisRegistryClient(client) {

  def this(configs: Map[String,String]) = this(ConfluentRegistryClient.createClient(configs))
}

object ConfluentRegistryClient {

  private def createClient(configs: Map[String,String]) = {
    val settings = new KafkaAvroDeserializerConfig(configs.asJava)
    val urls = settings.getSchemaRegistryUrls
    val maxSchemaObject = settings.getMaxSchemasPerSubject

    new CachedSchemaRegistryClient(urls, maxSchemaObject, configs.asJava)
  }

}
