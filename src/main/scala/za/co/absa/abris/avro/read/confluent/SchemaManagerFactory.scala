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


package za.co.absa.abris.avro.read.confluent

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.spark.internal.Logging
import za.co.absa.abris.avro.schemas.RegistryConfig

import scala.collection.JavaConverters._

/**
 * This factory allows us to mock the client for testing purposes
 */
object SchemaManagerFactory extends Logging {

  private var clientInstance: Option[SchemaRegistryClient] = None

  def setClientInstance(client: SchemaRegistryClient): Unit = {
    clientInstance = Option(client)
  }

  def resetClientInstance(): Unit = {
    clientInstance = None
  }

  def create(configs: Map[String,String]): SchemaManager = new SchemaManager(
    new RegistryConfig(configs),
    clientInstance.getOrElse(createRegistryClient(configs))
  )

  private def createRegistryClient(configs: Map[String,String]): SchemaRegistryClient = {

    val settings = new KafkaAvroDeserializerConfig(configs.asJava)

    val urls = settings.getSchemaRegistryUrls
    val maxSchemaObject = settings.getMaxSchemasPerSubject

    logInfo(msg = s"Configuring new Schema Registry instance of type " +
      s"'${classOf[CachedSchemaRegistryClient].getCanonicalName}'")

    new CachedSchemaRegistryClient(urls, maxSchemaObject, configs.asJava)
  }
}
