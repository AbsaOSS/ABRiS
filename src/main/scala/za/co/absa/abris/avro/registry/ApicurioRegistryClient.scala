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

import io.apicurio.registry.rest.client.{RegistryClient, RegistryClientFactory}
import io.apicurio.registry.types.ArtifactType
import io.confluent.kafka.schemaregistry.client.SchemaMetadata
import org.apache.avro.Schema
import za.co.absa.abris.avro.registry.ApicurioRegistryClient.{APICURIO_DEFAULT_GROUP_ID, AVRO_CONTENT_TYPE}
import za.co.absa.abris.config.AbrisConfig

import java.util
import javax.ws.rs.WebApplicationException
import scala.collection.JavaConverters._

class ApicurioRegistryClient(client: RegistryClient) extends AbrisRegistryClient {

  def this(configs: Map[String, String]) = this(ApicurioRegistryClient.createClient(configs))

  override def getAllVersions(subject: String): util.List[Integer] =
    client.listArtifactVersions(APICURIO_DEFAULT_GROUP_ID, subject, 0, Integer.MAX_VALUE)
      .getVersions.asScala.map(i => new Integer(i.getVersion.toInt)).asJava

  override def testCompatibility(subject: String, schema: Schema): Boolean = {
    try {
      val stream = new java.io.ByteArrayInputStream(
        schema.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8.name))
      client.testUpdateArtifact(APICURIO_DEFAULT_GROUP_ID, subject, AVRO_CONTENT_TYPE, stream)
      true
    } catch {
      case e: WebApplicationException => false
    }
  }

  override def register(subject: String, schema: Schema): Int = {
    val stream = new java.io.ByteArrayInputStream(
      schema.toString.getBytes(java.nio.charset.StandardCharsets.UTF_8.name))
    val artifactMetaData = client.createArtifact(APICURIO_DEFAULT_GROUP_ID, subject, ArtifactType.AVRO, stream)
    artifactMetaData.getContentId.toInt
  }

  override def getLatestSchemaMetadata(subject: String): SchemaMetadata = {
    val latestVersion = client.listArtifactVersions(APICURIO_DEFAULT_GROUP_ID, subject, 0, Integer.MAX_VALUE)
      .getVersions.asScala.last
    getSchemaMetadata(subject, latestVersion.getVersion.toInt)
  }

  override def getSchemaMetadata(subject: String, version: Int): SchemaMetadata = {
    val metadata = client.getArtifactVersionMetaData(APICURIO_DEFAULT_GROUP_ID, subject, version.toString)
    val schemaStream = client.getContentById(metadata.getContentId)
    val schemaString = scala.io.Source.fromInputStream(schemaStream).mkString

    new SchemaMetadata(metadata.getContentId.toInt, metadata.getVersion.toInt, schemaString)
  }

  override def getById(schemaId: Int): Schema = {
    val schemaStream = client.getContentById(schemaId.toLong)
    val schemaString = scala.io.Source.fromInputStream(schemaStream).mkString
    new Schema.Parser().parse(schemaString)
  }
}

object ApicurioRegistryClient {

  val APICURIO_DEFAULT_GROUP_ID = "default"
  val AVRO_CONTENT_TYPE = "application/json"

  private def createClient(configs: Map[String, String]) = {
    val conf = new util.HashMap[String, Object]()
    configs.foreach { case (key, value) => conf.put(key, value) }
    val registryUrl = conf.remove(AbrisConfig.SCHEMA_REGISTRY_URL).asInstanceOf[String]
    RegistryClientFactory.create(registryUrl, conf)
  }

}
