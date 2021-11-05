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

import org.apache.spark.internal.Logging
import za.co.absa.abris.avro.registry.{AbrisRegistryClient, ConfluentRegistryClient}
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.commons.annotation.DeveloperApi

import scala.collection.concurrent
import scala.util.control.NonFatal

/**
 * This thread-safe factory creates [[SchemaManager]] and also manages the instances of SchemaRegistryClient
 * used by allowing caching of the references in order to avoid creating instances in every call that can be
 * used to cache schemas.
 * This factory also allows us to use custom registry client via abris.registryClient.class property.
 */
object SchemaManagerFactory extends Logging {

  private val clientInstances: concurrent.Map[Map[String,String], AbrisRegistryClient] = concurrent.TrieMap()

  @DeveloperApi
  def addSRClientInstance(configs: Map[String, String], client: AbrisRegistryClient): Unit = {
    clientInstances.put(configs, client)
  }

  @DeveloperApi
  def resetSRClientInstance(): Unit = {
   clientInstances.clear()
  }

  def create(configs: Map[String,String]): SchemaManager = new SchemaManager(getOrCreateRegistryClient(configs))

  private def getOrCreateRegistryClient(configs: Map[String,String]): AbrisRegistryClient = {
    clientInstances.getOrElseUpdate(configs, {
      if (configs.contains(AbrisConfig.REGISTRY_CLIENT_CLASS)) {
        try {
          val clazz = Class.forName(configs(AbrisConfig.REGISTRY_CLIENT_CLASS))
          logInfo(msg = s"Configuring new Schema Registry client of type '${clazz.getCanonicalName}'")
          clazz
            .getDeclaredConstructor(classOf[Map[String, String]])
            .newInstance(configs)
            .asInstanceOf[AbrisRegistryClient]
        } catch {
          case e if NonFatal(e) =>
            throw new IllegalArgumentException("Custom registry client must implement AbrisRegistryClient " +
              "and have constructor accepting Map[String, String]", e)
        }
      } else {
        logInfo(msg = s"Configuring new Schema Registry client of type ConfluentRegistryClient")
        new ConfluentRegistryClient(configs)
      }
    })
  }
}
