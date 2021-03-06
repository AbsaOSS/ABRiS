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

import java.util
import io.confluent.common.config.ConfigException
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.spark.internal.Logging
import za.co.absa.abris.avro.registry.{AbrisMockSchemaRegistryClient, CustomRegistryClient}
import za.co.absa.abris.config.AbrisConfig

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

      if (hasValidMockURL(urls)) {
        logInfo(msg = s"Configuring new Schema Registry instance of type " +
          s"'${classOf[AbrisMockSchemaRegistryClient].getCanonicalName}'")

        new AbrisMockSchemaRegistryClient()
      } else if (configs.contains(AbrisConfig.REGISTRY_CLIENT_CLASS)) {
        val cl = Class.forName(configs(AbrisConfig.REGISTRY_CLIENT_CLASS))
        if (classOf[CustomRegistryClient].isAssignableFrom(cl)) {
          logInfo(msg = s"Configuring new Schema Registry instance of type " +
            s"'${cl.getCanonicalName}'")
          val instance = cl.getDeclaredConstructor(Array(classOf[util.Map[String, String]]): _*)
            .newInstance(configs.asJava)

          instance.asInstanceOf[CustomRegistryClient]
        } else {
          throw new IllegalArgumentException("Custom registry classes have to extend 'CustomRegistryClient'!")
        }
      } else {
        logInfo(msg = s"Configuring new Schema Registry instance of type " +
          s"'${classOf[CachedSchemaRegistryClient].getCanonicalName}'")

        new CachedSchemaRegistryClient(urls, maxSchemaObject, configs.asJava)
      }
    })
  }

  /**
   * Checks whether the urls contain a mock registry.
   * This is doing the same check as the confluent serializer does in order to
   * check if the user wants to use a mocked registry e.g. for testing.
   * @return true/false or an exception if the configuration is wrong
   */
  private def hasValidMockURL(urls: util.List[String]): Boolean = {
    val mockURLs = urls.asScala
      .filter(_.startsWith("mock://"))

    if (mockURLs.isEmpty) {
      false
    } else if (mockURLs.size > 1) {
      throw new ConfigException("Only one mock scope is permitted for 'schema.registry.url'. Got: " + urls)
    } else if (urls.size > mockURLs.size) {
      throw new ConfigException("Cannot mix mock and real urls for 'schema.registry.url'. Got: " + urls)
    } else {
      true
    }
  }
}
