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

package za.co.absa.abris.config

import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry._

object AbrisConfig {
  def toSimpleAvro: ToSimpleAvroConfigFragment = new ToSimpleAvroConfigFragment()
  def toConfluentAvro: ToConfluentAvroConfigFragment = new ToConfluentAvroConfigFragment()
  def fromSimpleAvro: FromSimpleAvroConfigFragment = new FromSimpleAvroConfigFragment()
  def fromConfluentAvro: FromConfluentAvroConfigFragment = new FromConfluentAvroConfigFragment()

  val SCHEMA_REGISTRY_URL = "schema.registry.url"
}

/*
 * ================================================  To Avro ================================================
 */

class ToSimpleAvroConfigFragment() {
  def downloadSchemaById(schemaId: Int): ToSchemaDownloadingConfigFragment =
    new ToSchemaDownloadingConfigFragment(new IdCoordinate(schemaId), false)

  def downloadSchemaByLatestVersion: ToStrategyConfigFragment =
    new ToStrategyConfigFragment(LatestVersion(), false)

  def downloadSchemaByVersion(schemaVersion: Int): ToStrategyConfigFragment =
    new ToStrategyConfigFragment(NumVersion(schemaVersion), false)

  def provideSchema(schema: String): ToAvroConfig =
    new ToAvroConfig(schema, None)

  def provideAndRegisterSchema(schema: String): ToConfluentAvroRegistrationStrategyConfigFragment =
    new ToConfluentAvroRegistrationStrategyConfigFragment(schema, false)
}

class ToStrategyConfigFragment(version: SchemaVersion, confluent: Boolean) {
  def andTopicNameStrategy(
    topicName: String,
    isKey: Boolean = false
  ): ToSchemaDownloadingConfigFragment =
    toSDCFragment(SchemaSubject.usingTopicNameStrategy(topicName, isKey))

  def andRecordNameStrategy(
    recordName: String,
    recordNamespace: String,
  ): ToSchemaDownloadingConfigFragment =
    toSDCFragment(SchemaSubject.usingRecordNameStrategy(recordName, recordNamespace))

  def andTopicRecordNameStrategy(
    topicName: String,
    recordName: String,
    recordNamespace: String,
  ): ToSchemaDownloadingConfigFragment =
    toSDCFragment(SchemaSubject.usingTopicRecordNameStrategy(topicName, recordName, recordNamespace))

  private def toSDCFragment(subject: SchemaSubject) =
    new ToSchemaDownloadingConfigFragment(SubjectCoordinate(subject, version), confluent)
}

class ToSchemaDownloadingConfigFragment(schemaCoordinates: SchemaCoordinate, confluent: Boolean) {
  def usingSchemaRegistry(url: String): ToAvroConfig = usingSchemaRegistry(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> url))
  def usingSchemaRegistry(config: Map[String, String]): ToAvroConfig = {
    val schemaManager = SchemaManagerFactory.create(config)
    val (schemaId, schemaString) = schemaCoordinates match {
      case ic: IdCoordinate => (ic.schemaId, schemaManager.getSchemaById(ic.schemaId).toString)
      case sc: SubjectCoordinate => {
        val metadata = schemaManager.getSchemaMetadataBySubjectAndVersion(sc.subject, sc.version)
        (metadata.getId, metadata.getSchema)
      }
    }
    new ToAvroConfig(schemaString, if (confluent) Some(schemaId) else None)
  }
}

class ToConfluentAvroConfigFragment() {
  def downloadSchemaById(schemaId: Int): ToSchemaDownloadingConfigFragment =
    new ToSchemaDownloadingConfigFragment(new IdCoordinate(schemaId), true)

  def downloadSchemaByLatestVersion: ToStrategyConfigFragment =
    new ToStrategyConfigFragment(LatestVersion(), true)

  def downloadSchemaByVersion(schemaVersion: Int): ToStrategyConfigFragment =
    new ToStrategyConfigFragment(NumVersion(schemaVersion), true)

  def provideAndRegisterSchema(schema: String): ToConfluentAvroRegistrationStrategyConfigFragment =
    new ToConfluentAvroRegistrationStrategyConfigFragment(schema, true)
}

class ToConfluentAvroRegistrationStrategyConfigFragment(schema: String, confluent: Boolean) {
  def usingTopicNameStrategy(
    topicName: String,
    isKey: Boolean = false
  ): ToSchemaRegisteringConfigFragment =
    toSRCFragment(SchemaSubject.usingTopicNameStrategy(topicName, isKey))

  def usingRecordNameStrategy(
  ): ToSchemaRegisteringConfigFragment =
    toSRCFragment(SchemaSubject.usingRecordNameStrategy(AvroSchemaUtils.parse(schema)))

  def usingTopicRecordNameStrategy(
    topicName: String,
  ): ToSchemaRegisteringConfigFragment =
    toSRCFragment(SchemaSubject.usingTopicRecordNameStrategy(topicName, AvroSchemaUtils.parse(schema)))

  private def toSRCFragment(subject: SchemaSubject) =
    new ToSchemaRegisteringConfigFragment(subject, schema, confluent)
}

class ToSchemaRegisteringConfigFragment(
  subject: SchemaSubject,
  schemaString: String,
  confluent: Boolean
) {
  def usingSchemaRegistry(url: String): ToAvroConfig = usingSchemaRegistry(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> url))
  def usingSchemaRegistry(config: Map[String, String]): ToAvroConfig = {
    val schemaManager = SchemaManagerFactory.create(config)
    val schema = AvroSchemaUtils.parse(schemaString)
    val schemaId = schemaManager.getIfExistsOrElseRegisterSchema(schema, subject)
    new ToAvroConfig(schemaString, if (confluent) Some(schemaId) else None)
  }
}

class ToAvroConfig(val schemaString: String, val schemaId: Option[Int])

/*
 * ================================================  From Avro ================================================
 */

class FromSimpleAvroConfigFragment{
  def downloadSchemaById(schemaId: Int): FromSchemaDownloadingConfigFragment =
    new FromSchemaDownloadingConfigFragment(Left(new IdCoordinate(schemaId)), false)

  def downloadSchemaByLatestVersion: FromStrategyConfigFragment =
    new FromStrategyConfigFragment(LatestVersion(), false)

  def downloadSchemaByVersion(schemaVersion: Int): FromStrategyConfigFragment =
    new FromStrategyConfigFragment(NumVersion(schemaVersion), false)

  def provideSchema(schema: String): FromAvroConfig =
    new FromAvroConfig(schema, None)
}

class FromStrategyConfigFragment(version: SchemaVersion, confluent: Boolean) {
  def andTopicNameStrategy(
    topicName: String,
    isKey: Boolean = false
  ): FromSchemaDownloadingConfigFragment =
    toFSDCFragment(SchemaSubject.usingTopicNameStrategy(topicName, isKey))

  def andRecordNameStrategy(
    recordName: String,
    recordNamespace: String,
  ): FromSchemaDownloadingConfigFragment =
    toFSDCFragment(SchemaSubject.usingRecordNameStrategy(recordName, recordNamespace))

  def andTopicRecordNameStrategy(
    topicName: String,
    recordName: String,
    recordNamespace: String,
  ): FromSchemaDownloadingConfigFragment =
    toFSDCFragment(SchemaSubject.usingTopicRecordNameStrategy(topicName, recordName, recordNamespace))

  private def toFSDCFragment(subject: SchemaSubject) =
    new FromSchemaDownloadingConfigFragment(Left(SubjectCoordinate(subject, version)), confluent)
}

class FromSchemaDownloadingConfigFragment(
  schemaCoordinatesOrSchemaString: Either[SchemaCoordinate, String],
  confluent: Boolean
) {
  def usingSchemaRegistry(url: String): FromAvroConfig =
    usingSchemaRegistry(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> url))

  def usingSchemaRegistry(config: Map[String, String]): FromAvroConfig = schemaCoordinatesOrSchemaString match {
    case Left(coordinate) => {
      val schemaManager = SchemaManagerFactory.create(config)
      val schema = schemaManager.getSchema(coordinate)
      new FromAvroConfig(schema.toString, if (confluent) Some(config) else None)
    }
    case Right(schemaString) =>
      if (confluent) {
        new FromAvroConfig(schemaString, Some(config))
      } else {
        throw new UnsupportedOperationException("Unsupported config permutation")
      }
  }
}

class FromConfluentAvroConfigFragment {
  def downloadReaderSchemaById(schemaId: Int): FromSchemaDownloadingConfigFragment =
    new FromSchemaDownloadingConfigFragment(Left(new IdCoordinate(schemaId)), true)

  def downloadReaderSchemaByLatestVersion: FromStrategyConfigFragment =
    new FromStrategyConfigFragment(LatestVersion(), true)

  def downloadReaderSchemaByVersion(schemaVersion: Int): FromStrategyConfigFragment =
    new FromStrategyConfigFragment(NumVersion(schemaVersion), true)

  def provideReaderSchema(schema: String): FromSchemaDownloadingConfigFragment =
    new FromSchemaDownloadingConfigFragment(Right(schema), true)
}


class FromAvroConfig(val schemaString: String, val schemaRegistryConf: Option[Map[String,String]])
