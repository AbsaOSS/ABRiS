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

import za.co.absa.abris.avro.errors.DeserializationExceptionHandler
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManagerFactory
import za.co.absa.abris.avro.registry._

object AbrisConfig {
  def toSimpleAvro: ToSimpleAvroConfigFragment = new ToSimpleAvroConfigFragment()
  def toConfluentAvro: ToConfluentAvroConfigFragment = new ToConfluentAvroConfigFragment()
  def fromSimpleAvro: FromSimpleAvroConfigFragment = new FromSimpleAvroConfigFragment()
  def fromConfluentAvro: FromConfluentAvroConfigFragment = new FromConfluentAvroConfigFragment()

  val SCHEMA_REGISTRY_URL = "schema.registry.url"
  val REGISTRY_CLIENT_CLASS = "abris.registryClient.class"
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
    recordNamespace: String
  ): ToSchemaDownloadingConfigFragment =
    toSDCFragment(SchemaSubject.usingRecordNameStrategy(recordName, recordNamespace))

  def andTopicRecordNameStrategy(
    topicName: String,
    recordName: String,
    recordNamespace: String
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
    topicName: String
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

/**
  * This class serves as self sufficient builder for Abris configuration while still being fully backward compatible
  * with previous Fragment based config builders above.
  *
  * This builder allows us to add new properties in backward compatible manner.
  */
@SerialVersionUID(1L)
class ToAvroConfig private(abrisConfig: Map[String, Any]) extends Serializable {

  import ToAvroConfig.Key

  // ### legacy methods ###

  def this(schemaString: String, schemaId: Option[Int]) =
    this(
      schemaId.map(ToAvroConfig.Key.SchemaId -> _).toMap[String, Any] + (ToAvroConfig.Key.Schema -> schemaString)
    )

  private[abris] def schemaString(): String = abrisConfig(Key.Schema).asInstanceOf[String]
  private[abris] def schemaId(): Option[Int] = abrisConfig.get(Key.SchemaId).map(_.asInstanceOf[Int])

  // ### normal methods ###

  private[abris] def abrisConfig(): Map[String, Any] = abrisConfig

  /**
    * @param schema Mandatory avro schema for converting to avro format
    */
  def withSchema(schema: String): ToAvroConfig =
    new ToAvroConfig(
      abrisConfig + (Key.Schema -> schema)
    )

  /**
    * @param schemaId Schema id that will be prepended before the avro payload (making it confluent compliant)
    */
  def withSchemaId(schemaId: Int): ToAvroConfig =
    new ToAvroConfig(
      abrisConfig + (Key.SchemaId -> schemaId)
    )

  def validate(): Unit = {
    if (!abrisConfig.contains(Key.Schema)) {
      throw new IllegalArgumentException(s"Missing mandatory config property ${Key.Schema}")
    }
  }
}

object ToAvroConfig {
  def apply(): ToAvroConfig = new ToAvroConfig(Map.empty[String, Any])

  private[abris] object Key {
    val Schema = "schema"
    val SchemaId = "schemaId"
  }
}

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
    recordNamespace: String
  ): FromSchemaDownloadingConfigFragment =
    toFSDCFragment(SchemaSubject.usingRecordNameStrategy(recordName, recordNamespace))

  def andTopicRecordNameStrategy(
    topicName: String,
    recordName: String,
    recordNamespace: String
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

/**
  * This class serves as self sufficient builder for Abris configuration while still being fully backward compatible
  * with previous Fragment based config builders above.
  *
  * This builder allows us to add new properties in backward compatible manner.
  */
@SerialVersionUID(1L)
class FromAvroConfig private(
  abrisConfig: Map[String, Any],
  schemaRegistryConf: Option[Map[String,String]]
) extends Serializable {

  import FromAvroConfig.Key

  // ### legacy methods ###

  def this(schemaString: String, schemaRegistryConf: Option[Map[String,String]]) =
    this(Map(FromAvroConfig.Key.ReaderSchema -> schemaString), schemaRegistryConf)

  private[abris] def schemaString(): String = abrisConfig(Key.ReaderSchema).asInstanceOf[String]
  private[abris] def schemaRegistryConf(): Option[Map[String,String]] = schemaRegistryConf

  // ### normal methods ###

  private[abris] def abrisConfig(): Map[String, Any] = abrisConfig

  /**
    * @param srConfig Schema registry client config map
    *   - providing this means the confluent compliant messages are expected
    *   - the id from the message will be used to download an avro schema
    *     using the client with this configuration.
    *   - the downloaded schema will be used as a writer schema
    */
  def withSchemaRegistryConfig(srConfig: Map[String,String]): FromAvroConfig =
    new FromAvroConfig(
      abrisConfig,
      Some(srConfig)
    )

  /**
    * @param schema Reader schema used for converting from avro
    */
  def withReaderSchema(schema: String): FromAvroConfig =
    new FromAvroConfig(
      abrisConfig + (Key.ReaderSchema -> schema),
      schemaRegistryConf
    )

  /**
    * @param schema Writer schema used for converting from avro
    */
  def withWriterSchema(schema: String): FromAvroConfig =
    new FromAvroConfig(
      abrisConfig + (Key.WriterSchema -> schema),
      schemaRegistryConf
    )

  def withSchemaConverter(schemaConverter: String): FromAvroConfig =
    new FromAvroConfig(
      abrisConfig + (Key.SchemaConverter -> schemaConverter),
      schemaRegistryConf
    )

  /**
   * @param exceptionHandler exception handler used for converting from avro
   */
  def withExceptionHandler(exceptionHandler: DeserializationExceptionHandler): FromAvroConfig =
    new FromAvroConfig(
      abrisConfig + (Key.ExceptionHandler -> exceptionHandler),
      schemaRegistryConf
    )

  def validate(): Unit = {
    if(!abrisConfig.contains(Key.ReaderSchema)) {
      throw new IllegalArgumentException(s"Missing mandatory config property ${Key.ReaderSchema}")
    }
    if(schemaRegistryConf.isDefined && abrisConfig.contains(Key.WriterSchema)) {
      throw new IllegalArgumentException(s"Either schemaRegistryConf or ${Key.WriterSchema} should be set. Not both!")
    }
  }
}

object FromAvroConfig {
  def apply(): FromAvroConfig = new FromAvroConfig(Map.empty[String, Any], None)

  private[abris] object Key {
    val ReaderSchema = "readerSchema"
    val WriterSchema = "writerSchema"
    val SchemaConverter = "schemaConverter"
    val ExceptionHandler = "exceptionHandler"
  }
}
