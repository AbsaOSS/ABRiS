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

import scala.collection.JavaConverters._
import scala.collection.concurrent

/**
 * This thread-safe factory creates [[SchemaManager]] and also manages the instances of SchemaRegistryClient
 * used by allowing caching of the references in order to avoid creating instances in every call that can be
 * used to cache schemas.
 * This factory also allows us to mock the client for testing purposes.
 */
object SchemaManagerFactory extends Logging {

  private val clientInstances: concurrent.Map[Map[String,String], SchemaRegistryClient] = concurrent.TrieMap()

  def addSRClientInstance(configs: Map[String, String], client: SchemaRegistryClient): Unit = {
    clientInstances.put(configs, client)
  }

  def resetSRClientInstance(): Unit = {
   clientInstances.clear()
  }

  def create(configs: Map[String,String]): SchemaManager = new SchemaManager(getOrCreateRegistryClient(configs))

  private def getOrCreateRegistryClient(configs: Map[String,String]): SchemaRegistryClient = {
    clientInstances.getOrElseUpdate(configs, {
      val settings = new KafkaAvroDeserializerConfig(configs.asJava)
      val urls = settings.getSchemaRegistryUrls
      val maxSchemaObject = settings.getMaxSchemasPerSubject

      logInfo(msg = s"Configuring new Schema Registry instance of type " +
        s"'${classOf[CachedSchemaRegistryClient].getCanonicalName}'")

      new CachedSchemaRegistryClient(urls, maxSchemaObject, configs.asJava)
    })
  }

}
