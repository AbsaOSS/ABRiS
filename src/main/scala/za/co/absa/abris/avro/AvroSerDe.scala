/*
 * Copyright 2018 Barclays Africa Group Limited
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

package za.co.absa.abris.avro

import java.security.InvalidParameterException

import org.apache.avro.Schema
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row}
import org.slf4j.LoggerFactory
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.ScalaConfluentKafkaAvroDeserializer
import za.co.absa.abris.avro.schemas.SchemasProcessor
import za.co.absa.abris.avro.schemas.impl.{AvroToSparkProcessor, SparkToAvroProcessor}
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.{RETAIN_ORIGINAL_SCHEMA, RETAIN_SELECTED_COLUMN_ONLY, SchemaRetentionPolicy}
import za.co.absa.abris.avro.serde.AvroDecoder
/**
 * This object provides the main point of integration between applications and this library.
  *
  * It is STRONGLY recommended that users of this library understand the concept behind [[za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies]].
 */
object AvroSerDe {

  private val logger = LoggerFactory.getLogger(AvroSerDe.getClass)

  /**
   * This class provides the method that converts binary Avro records from a Dataframe into Spark Rows on the fly.
   *
   * It loads binary data from a stream and feed them into an Avro/Spark decoder, returning the resulting rows.
   *
   * It requires the path to the Avro schema which defines the records to be read.
   */
  implicit class DataframeDeserializer(dataframe: Dataset[Row]) extends AvroDecoder {

    /**
      * Converts using an instantiated Schema.
      */
    def fromAvro(column: String, schema: Schema)(schemaRetentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      schemaRetentionPolicy match {
        case RETAIN_SELECTED_COLUMN_ONLY => fromAvroToRow(getBatchDataFromColumn(column), schema)
        case RETAIN_ORIGINAL_SCHEMA      => fromAvroToRow(dataframe, schema, column)
        case _ => throw new IllegalArgumentException(s"Invalid Schema Retention Policy: $schemaRetentionPolicy")
      }
    }

    /**
      * Loads the schema from a file.
      */
    def fromAvro(column: String, schemaPath: String)(schemaRetentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      schemaRetentionPolicy match {
        case RETAIN_SELECTED_COLUMN_ONLY => fromAvroToRow(getBatchDataFromColumn(column), schemaPath)
        case RETAIN_ORIGINAL_SCHEMA      => fromAvroToRow(dataframe, schemaPath, column)
        case _ => throw new IllegalArgumentException(s"Invalid Schema Retention Policy: $schemaRetentionPolicy")
      }
    }

    /**
      * Loads the schema from Schema Registry.
      */
    def fromAvro(column: String, schemaRegistryConf: Map[String,String])(schemaRetentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      schemaRetentionPolicy match {
        case RETAIN_SELECTED_COLUMN_ONLY => fromAvroToRow(getBatchDataFromColumn(column), schemaRegistryConf)
        case RETAIN_ORIGINAL_SCHEMA      => fromAvroToRow(dataframe, schemaRegistryConf, column)
        case _ => throw new IllegalArgumentException(s"Invalid Schema Retention Policy: $schemaRetentionPolicy")
      }
    }

    /**
      * This method supports schema changes from Schema Registry. However, the conversion between Avro records and Spark
      * rows relies on RowEncoders, which are defined before the job starts. Thus, although the schema changes are supported
      * while reading, they are not translated to RowEncoders, which could take to errors in the final data.
      *
      * Refer to the [[ScalaConfluentKafkaAvroDeserializer.deserialize()]] documentation to better understand how this
      * operation is performed.
      */
    def fromConfluentAvro(column: String, schemaPath: Option[String], schemaRegistryConf: Option[Map[String,String]])(schemaRetentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      schemaRetentionPolicy match {
        case RETAIN_SELECTED_COLUMN_ONLY => fromConfluentAvroToRow(getBatchDataFromColumn(column), schemaPath, schemaRegistryConf)
        case RETAIN_ORIGINAL_SCHEMA      => fromConfluentAvroToRow(dataframe, schemaPath, schemaRegistryConf, column)
        case _ => throw new IllegalArgumentException(s"Invalid Schema Retention Policy: $schemaRetentionPolicy")
      }
    }

    private def getBatchDataFromColumn(column: String) = dataframe.select(column)
  }

  /**
   * This class provides the method that converts binary Avro records from a stream into Spark Rows on the fly.
   *
   * It loads binary data from a stream and feed them into an Avro/Spark decoder, returning the resulting rows.
   *
   * It requires the path to the Avro schema which defines the records to be read.
   */
  implicit class StreamDeserializer(dsReader: DataStreamReader) extends AvroDecoder {

    /**
      * Converts used an instantiated Schema.
      */
    def fromAvro(column: String, schema: Schema)(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      retentionPolicy match {
        case RETAIN_SELECTED_COLUMN_ONLY => fromAvroToRow(getStreamDataFromColumn(column), schema)
        case RETAIN_ORIGINAL_SCHEMA      => fromAvroToRow(getStreamData(), schema, column)
        case _ => throw new IllegalArgumentException(s"Invalid Schema Retention Policy: $retentionPolicy")
      }
    }

    /**
      * Loads the schema from a file system.
      */
    def fromAvro(column: String, schemaPath: String)(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      retentionPolicy match {
        case RETAIN_SELECTED_COLUMN_ONLY => fromAvroToRow(getStreamDataFromColumn(column), schemaPath)
        case RETAIN_ORIGINAL_SCHEMA      => fromAvroToRow(getStreamData(), schemaPath, column)
        case _ => throw new IllegalArgumentException(s"Invalid Schema Retention Policy: $retentionPolicy")
      }
    }

    /**
      * Loads the schema from Schema Registry.
      */
    def fromAvro(column: String, schemaRegistryConf: Map[String,String])(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      retentionPolicy match {
        case RETAIN_SELECTED_COLUMN_ONLY => fromAvroToRow(getStreamDataFromColumn(column), schemaRegistryConf)
        case RETAIN_ORIGINAL_SCHEMA      => fromAvroToRow(getStreamData(), schemaRegistryConf, column)
        case _ => throw new IllegalArgumentException(s"Invalid Schema Retention Policy: $retentionPolicy")
      }
    }

    /**
      * This method supports schema changes from Schema Registry. However, the conversion between Avro records and Spark
      * rows relies on RowEncoders, which are defined before the job starts. Thus, although the schema changes are supported
      * while reading, they are not translated to RowEncoders, which could take to errors in the final data.
      *
      * Refer to the [[ScalaConfluentKafkaAvroDeserializer.deserialize()]] documentation to better understand how this
      * operation is performed.
      */
    def fromConfluentAvro(column: String, schemaPath: Option[String], confluentConf: Option[Map[String,String]])(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      retentionPolicy match {
        case RETAIN_SELECTED_COLUMN_ONLY => fromConfluentAvroToRow(getStreamDataFromColumn(column), schemaPath, confluentConf)
        case RETAIN_ORIGINAL_SCHEMA      => fromConfluentAvroToRow(getStreamData(), schemaPath, confluentConf, column)
        case _ => throw new IllegalArgumentException(s"Invalid Schema Retention Policy: $retentionPolicy")
      }
    }

    private def getStreamDataFromColumn(column: String) = dsReader.load.select(column)

    private def getStreamData() = dsReader.load
  }

  /**
   * This class provides methods to perform the translation from Dataframe Rows into Avro records on the fly.
   *
   * Users can either, inform the path to the destination Avro schema or inform record name and namespace and the schema
   * will be inferred from the Dataframe.
   *
   * The methods are "storage-agnostic", which means they provide Dataframes of Avro records which can be stored into any
   * sink (e.g. Kafka, Parquet, etc).
   */
  implicit class Serializer(dataframe: Dataset[Row]) {

    /**
      * Checks if the incoming Dataframe has a schema.
      *
      * @throws InvalidParameterException in case the schema is not set.
      */
    @throws[InvalidParameterException]
    private def checkDataframeSchema(): Boolean = {
      if (dataframe.schema == null || dataframe.schema.isEmpty) {
        throw new InvalidParameterException("Dataframe does not have a schema.")
      }
      true
    }

    /**
      * Tries to manage schema registration in case credentials to access Schema Registry are provided.
      *
      * This method either works or throws an InvalidParameterException.
      */
    @throws[InvalidParameterException]
    private def manageSchemaRegistration(topic: String, schema: Schema, schemaRegistryConf: Map[String,String]): Int = {

      val schemaId = AvroSchemaUtils.registerIfCompatibleSchema(topic, schema, schemaRegistryConf)

      if (schemaId.isEmpty) {
        throw new InvalidParameterException(s"Schema could not be registered for topic '$topic'. Make sure that the Schema Registry " +
            s"is available, the parameters are correct and the schemas ar compatible")
      }
      else {
        logger.info(s"Schema successfully registered for topic '$topic' with id '{${schemaId.get}}'.")
      }

      schemaId.get
    }

    /**
     * Converts from Dataset[Row] into Dataset[Array[Byte]] containing Avro records.
     *
     * Intended to be used when there is not Spark schema available in the Dataframe but there is an expected Avro schema.
     *
     * It is important to keep in mind that the specification for a field in the schema MUST be the same at both ends, writer and reader.
     * For some fields (e.g. strings), Spark can ignore the nullability specified in the SQL struct (SPARK-14139). This issue could lead
     * to fields being ignored. Thus, it is important to check the final SQL schema after Spark has created the Dataframes.
     *
     * For instance, the Spark construct 'StructType("name", StringType, false)' translates to the Avro field {"name": "name", "type":"string"}.
     * However, if Spark changes the nullability (StructType("name", StringType, TRUE)), the Avro field becomes a union: {"name":"name", "type": ["string", "null"]}.
     *
     * The difference in the specifications will prevent the field from being correctly loaded by Avro readers, leading to data loss.
     */
    def toAvro(schemaPath: String): Dataset[Array[Byte]] = {
      toAvro(AvroSchemaUtils.load(schemaPath))
    }

    def toAvro(schema: Schema): Dataset[Array[Byte]] = {
      val plainAvroSchema = schema.toString
      toAvro(dataframe, new AvroToSparkProcessor(plainAvroSchema))(None)
    }

    /**
      * Converts from Dataset[Row] into Dataset[Array[Byte]] containing Avro records.
      *
      * Intended to be used when there is a Spark schema present in the Dataframe from which the Avro schema will be translated.
      *
      * The API will infer the Avro schema from the incoming Dataframe. The inferred schema will receive the name and namespace informed as parameters.
      *
      * The inferred schema will be registered with Schema Registry in case the parameters for accessing it are provided.
      *
      * The API will throw in case the Dataframe does not have a schema or if Schema Registry access details are provided but the schema could not be registered due to
      * either incompatibility, wrong credentials or Schema Registry unavailability.
      *
      * Differently than the other API, this one does not suffer from the schema changing issue, since the final Avro schema will be derived from the schema
      * already used by Spark.
      */
    def toAvro(topic: String, schemaName: String, schemaNamespace: String)(schemaRegistryConf: Option[Map[String,String]] = None): Dataset[Array[Byte]] = {

      checkDataframeSchema()
      val schemaProcessor = new SparkToAvroProcessor(dataframe.schema, schemaName, schemaNamespace)

      if (schemaRegistryConf.isDefined) {
        manageSchemaRegistration(topic, schemaProcessor.getAvroSchema(), schemaRegistryConf.get)
      }

      toAvro(dataframe, schemaProcessor)(None)
    }

    /**
      * Converts from Dataset[Row] into Dataset[Array[Byte]] containing Avro records with the id of the schema in Schema Registry attached to the beginning of the payload.
      *
      * Intended to be used when there is a Spark schema present in the Dataframe from which the Avro schema will be translated.
      *
      * The API will infer the Avro schema from the incoming Dataframe. The inferred schema will receive the name and namespace informed as parameters.
      *
      * The inferred schema will be registered with Schema Registry in case the parameters for accessing it are provided.
      *
      * The API will throw in case the Dataframe does not have a schema or if Schema Registry access details are provided but the schema could not be registered due to
      * either incompatibility, wrong credentials or Schema Registry unavailability.
      *
      * Differently than the other API, this one does not suffer from the schema changing issue, since the final Avro schema will be derived from the schema
      * already used by Spark.
      */
    def toConfluentAvro(topic: String, schemaName: String, schemaNamespace: String)(schemaRegistryConf: Map[String,String]): Dataset[Array[Byte]] = {

      if (schemaRegistryConf.isEmpty) {
        throw new IllegalArgumentException("No Schema Registry connection parameters found. It is mandatory for this API entry to connect to Schema Registry so that" +
          " the inferred schema can be registered or updated and its id can be retrieved to be sent along with the payload.")
      }

      checkDataframeSchema()

      val schemaProcessor = new SparkToAvroProcessor(dataframe.schema, schemaName, schemaNamespace)

      val schemaId = manageSchemaRegistration(topic, schemaProcessor.getAvroSchema(), schemaRegistryConf)

      toAvro(dataframe, schemaProcessor)(Some(schemaId))
    }

    /**
      * Converts from Dataset[Row] into Dataset[Array[Byte]] containing Avro records with the id of the schema in Schema Registry attached to the beginning of the payload.
      *
      * Intended to be used when users want to force an Avro schema on the Dataframe, instead of having it inferred.
      *
      * The informed schema will be registered with Schema Registry in case the parameters for accessing it are provided an it is compatible with the currently registered version.
      *
      * The API will throw if Schema Registry access details are provided but the schema could not be registered due to either incompatibility, wrong credentials or Schema Registry unavailability.
      */
    def toConfluentAvro(topic: String, schemaPath: String)(schemaRegistryConf: Map[String,String]): Dataset[Array[Byte]] = {

      if (schemaRegistryConf.isEmpty) {
        throw new IllegalArgumentException("No Schema Registry connection parameters found. It is mandatory for this API entry to connect to Schema Registry so that" +
          " the inferred schema can be registered or updated and its id can be retrieved to be sent along with the payload.")
      }

      checkDataframeSchema()

      val schema = AvroSchemaUtils.load(schemaPath)

      val schemaProcessor = new AvroToSparkProcessor(schema.toString)

      val schemaId = manageSchemaRegistration(topic, schemaProcessor.getAvroSchema(), schemaRegistryConf)

      toAvro(dataframe, schemaProcessor)(Some(schemaId))
    }

    /**
     * Converts from Dataset[Row] into Dataset[Array[Byte]] containing Avro records.
     *
     * Intended to be used when there is a Spark schema present in the Dataframe from which the Avro schema will be translated.
     *
     * The API will infer the Avro schema from the incoming Dataframe. The inferred schema will receive the name and namespace informed as parameters.
     *
     * The API will throw in case the Dataframe does not have a schema.
     *
     * Differently than the other API, this one does not suffer from the schema changing issue, since the final Avro schema will be derived from the schema
     * already used by Spark.
     */
    def toAvro(schemaName: String, schemaNamespace: String): Dataset[Array[Byte]] = {
      checkDataframeSchema()
      toAvro(dataframe, new SparkToAvroProcessor(dataframe.schema, schemaName, schemaNamespace))(None)
    }

    /**
     * Converts a Dataset[Row] into a Dataset[Array[Byte]] containing Avro schemas generated according to the plain specification informed as a parameter.
     */
    private def toAvro(rows: Dataset[Row], schemas: SchemasProcessor)(schemaId: Option[Int]): Dataset[Array[Byte]] = {
      implicit val recEncoder: Encoder[Array[Byte]] = Encoders.BINARY
      rows.mapPartitions(partition => {
        val avroSchema = schemas.getAvroSchema()
        val sparkSchema = schemas.getSparkSchema()
        partition.map(row => SparkAvroConversions.rowToBinaryAvro(row, sparkSchema, avroSchema, schemaId))
      })
    }
  }
}