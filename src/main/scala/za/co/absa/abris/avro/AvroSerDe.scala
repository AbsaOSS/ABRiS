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
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.slf4j.LoggerFactory
import za.co.absa.abris.avro.format.{ScalaAvroRecord, SparkAvroConversions}
import za.co.absa.abris.avro.parsing.AvroToSparkParser
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.ScalaDatumReader
import za.co.absa.abris.avro.read.confluent.{ScalaConfluentKafkaAvroDeserializer, SchemaManager}
import za.co.absa.abris.avro.schemas.SchemasProcessor
import za.co.absa.abris.avro.schemas.impl.{AvroToSparkProcessor, SparkToAvroProcessor}

import scala.reflect.ClassTag
/**
 * This object provides the main point of integration between applications and this library.
 */
object AvroSerDe {

  private val logger = LoggerFactory.getLogger(AvroSerDe.getClass)

  private val avroParser = new AvroToSparkParser()
  private var reader: ScalaDatumReader[ScalaAvroRecord] = _

  /**
   * Receives binary Avro records and converts them into Spark Rows.
   */
  private def decodeAvro[T](avroRecord: Array[Byte])(implicit tag: ClassTag[T]): Row = {
    val decoder = DecoderFactory.get().binaryDecoder(avroRecord, null)
    val decodedAvroData: GenericRecord = reader.read(null, decoder)

    avroParser.parse(decodedAvroData)
  }

  /**
    * Parses an Avro GenericRecord into a Spark row.
    */
  private def decodeAvro[T](avroRecord: GenericRecord)(implicit tag: ClassTag[T]): Row = {
    avroParser.parse(avroRecord)
  }

  /**
    * Creates an instance of ScalaDatumReader for the schema informed.
    */
  private def createAvroReader(schemaPath: String): Unit = {
    createAvroReader(AvroSchemaUtils.load(schemaPath))
  }

  /**
    * Creates an instance of ScalaDatumReader for the schema informed.
    */
  private def createAvroReader(schema: Schema): Unit = {
    reader = new ScalaDatumReader[ScalaAvroRecord](schema)
  }

  /**
    * Creates an instance of [[ScalaConfluentKafkaAvroDeserializer]] and configures its Schema Registry access in case
    * the parameters to do it are defined.
    */
  private def createConfiguredConfluentAvroReader(schemaPath: Option[String], schemaRegistryConf: Option[Map[String,String]]): ScalaConfluentKafkaAvroDeserializer = {
    val schema = if (schemaPath.isDefined) Some(AvroSchemaUtils.load(schemaPath.get)) else None
    val configs = if (schemaRegistryConf.isDefined) schemaRegistryConf.get else Map[String,String]()
    val topic = if (configs.contains(SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC)) Some(configs(SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC)) else None

    val reader = new ScalaConfluentKafkaAvroDeserializer(topic, schema)
    reader.configureSchemaRegistry(configs)
    reader
  }

  private def createRowEncoder(schema: Schema) = {
    RowEncoder(SparkAvroConversions.toSqlType(schema))
  }

  private def createRowEncoder(schemaPath: String): ExpressionEncoder[Row] = {
    createRowEncoder(AvroSchemaUtils.load(schemaPath))
  }

  private def createRowEncoder(schemaRegistryConf: Map[String,String]): ExpressionEncoder[Row] = {
    createRowEncoder(AvroSchemaUtils.load(schemaRegistryConf))
  }

  /**
   * Converts binary Avro records into Spark Rows.
   */
  private[avro] class AvroRowConverter {

    /**
      * Converts Dataframes of binary Avro records into Dataframes of type Spark data.
      *
      * Highlights:
      *
      * 1. Either, the path to a schema stored in a file system or the configuration to access a Confluent's Schema Registry
      *    instance must be informed.
      *
      * 2. The RowEncoder for the resulting Dataframes will be created here, thus. If a schema path is informed, the schema
      *    under that path will be used to create the RowEncoder, otherwise, the schema retrieved from Schema Registry will
      *    be used.
      *
      *    To allow the retrieval of a remote schema, the API will look into the configurations for:
      *
      *    a. the topic name
      *    b. the Schema Registry URLs
      *    c. The schema version
      */
    protected def fromConfluentAvroToRow(dataframe: Dataset[Row], schemaPath: Option[String], schemaRegistryConf: Option[Map[String,String]]) = {

      if (schemaPath.isEmpty && schemaRegistryConf.isEmpty) {
        throw new InvalidParameterException("Neither path to schema in file system nor Schema Registry configurations was provided.")
      }

      implicit val rowEncoder = if (schemaRegistryConf.isDefined) {
        createRowEncoder(schemaRegistryConf.get)
      }
      else {
        createRowEncoder(schemaPath.get)
      }

      dataframe
        .as(Encoders.BINARY)
        .mapPartitions(partition => {
          val reader = createConfiguredConfluentAvroReader(schemaPath, schemaRegistryConf)
          partition.map(avroRecord => {
            decodeAvro(reader.deserialize(avroRecord))
          })
        })
    }

    /**
      * Converts the binary Avro records contained in the Dataframe into regular Rows with a
      * SQL schema whose specification is translated from the Avro schema informed.
      */
    protected def fromAvroToRow(dataframe: Dataset[Row], schema: Schema): Dataset[Row] = {

      implicit val rowEncoder = createRowEncoder(schema)

      // transforming to plain and reparsing inside mapping because Schema instances are not serializable.
      val plainSchema = schema.toString

      dataframe
        .as(Encoders.BINARY)
        .mapPartitions(partition => {
          createAvroReader(AvroSchemaUtils.parse(plainSchema))
          partition.map(avroRecord => {
            decodeAvro(avroRecord)
          })
        })
    }

    /**
     * Converts the binary Avro records contained in the Dataframe into regular Rows with a
     * SQL schema whose specification is translated from the Avro schema informed.
     */
    protected def fromAvroToRow(dataframe: Dataset[Row], schemaPath: String): Dataset[Row] = {
      fromAvroToRow(dataframe, AvroSchemaUtils.load(schemaPath))
    }

    /**
      * Converts the binary Avro records contained in the Dataframe into regular Rows with a
      * SQL schema whose specification is translated from the Avro schema informed.
      */
    protected def fromAvroToRow(dataframe: Dataset[Row], schemaRegistryConf: Map[String,String]) = {

      val schema = AvroSchemaUtils.load(schemaRegistryConf)
      implicit val rowEncoder = createRowEncoder(schema)

      // has to convert into String and re-parse it inside the 'map' operation since Avro Schema instances are not serializable
      val plainSchema = schema.toString()

      dataframe
        .as(Encoders.BINARY)
        .mapPartitions(partition => {
          createAvroReader(AvroSchemaUtils.parse(plainSchema))
          partition.map(avroRecord => {
            decodeAvro(avroRecord)
          })
        })
    }
  }

  /**
   * This class provides the method that converts binary Avro records from a Dataframe into Spark Rows on the fly.
   *
   * It loads binary data from a stream and feed them into an Avro/Spark decoder, returning the resulting rows.
   *
   * It requires the path to the Avro schema which defines the records to be read.
   */
  implicit class DataframeDeserializer(dataframe: Dataset[Row]) extends AvroRowConverter {

    /**
      * Converts used an instantiated Schema.
      */
    def fromAvro(schema: Schema) = {
      fromAvroToRow(getBatchData(), schema)
    }

    /**
      * Loads the schema from a file.
      */
    def fromAvro(schemaPath: String) = {
      fromAvroToRow(getBatchData(), schemaPath)
    }

    /**
      * Loads the schema from Schema Registry.
      */
    def fromAvro(schemaRegistryConf: Map[String,String]) = {
      fromAvroToRow(getBatchData(), schemaRegistryConf)
    }

    /**
      * This method supports schema changes from Schema Registry. However, the conversion between Avro records and Spark
      * rows relies on RowEncoders, which are defined before the job starts. Thus, although the schema changes are supported
      * while reading, they are not translated to RowEncoders, which could take to errors in the final data.
      *
      * Refer to the [[ScalaConfluentKafkaAvroDeserializer.deserialize()]] documentation to better understand how this
      * operation is performed.
      */
    def fromConfluentAvro(schemaPath: Option[String], schemaRegistryConf: Option[Map[String,String]]) = {
      fromConfluentAvroToRow(getBatchData(), schemaPath, schemaRegistryConf)
    }

    private def getBatchData() = dataframe.select("value")
  }

  /**
   * This class provides the method that converts binary Avro records from a stream into Spark Rows on the fly.
   *
   * It loads binary data from a stream and feed them into an Avro/Spark decoder, returning the resulting rows.
   *
   * It requires the path to the Avro schema which defines the records to be read.
   */
  implicit class StreamDeserializer(dsReader: DataStreamReader) extends AvroRowConverter {

    /**
      * Converts used an instantiated Schema.
      */
    def fromAvro(schema: Schema) = {
      fromAvroToRow(getStreamData(), schema)
    }

    /**
      * Loads the schema from a file system.
      */
    def fromAvro(schemaPath: String) = {
      fromAvroToRow(getStreamData(), schemaPath)
    }

    /**
      * Loads the schema from Schema Registry.
      */
    def fromAvro(schemaRegistryConf: Map[String,String]) = {
      fromAvroToRow(getStreamData(), schemaRegistryConf)
    }

    /**
      * This method supports schema changes from Schema Registry. However, the conversion between Avro records and Spark
      * rows relies on RowEncoders, which are defined before the job starts. Thus, although the schema changes are supported
      * while reading, they are not translated to RowEncoders, which could take to errors in the final data.
      *
      * Refer to the [[ScalaConfluentKafkaAvroDeserializer.deserialize()]] documentation to better understand how this
      * operation is performed.
      */
    def fromConfluentAvro(schemaPath: Option[String], confluentConf: Option[Map[String,String]]) = {
      fromConfluentAvroToRow(getStreamData(), schemaPath, confluentConf)
    }

    private def getStreamData() = dsReader.load.select("value")
  }

  /**
   * This class provides methods to perform the translation from Dataframe Rows into Avro records on the fly.
   *
   * Users can either, inform the path to the destination Avro schema or inform record name and namespace and the schema
   * will be inferred from the Dataframe.
   *
   * The methods are "storage-agnostic", which means the provide Dataframes of Avro records which can be stored into any
   * sink (e.g. Kafka, Parquet, etc).
   */
  implicit class Serializer(dataframe: Dataset[Row]) {

    /**
      * Checks if the incoming Dataframe has a schema.
      *
      * @throws InvalidParameterException in case the schema is not set.
      */
    @throws[InvalidParameterException]
    private def checkDataframeSchema() = {
      if (dataframe.schema == null || dataframe.schema.isEmpty) {
        throw new InvalidParameterException("Dataframe does not have a schema.")
      }
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
      implicit val recEncoder = Encoders.BINARY
      rows.mapPartitions(partition => {
        val avroSchema = schemas.getAvroSchema()
        val sparkSchema = schemas.getSparkSchema()
        partition.map(row => SparkAvroConversions.rowToBinaryAvro(row, sparkSchema, avroSchema, schemaId))
      })
    }
  }
}