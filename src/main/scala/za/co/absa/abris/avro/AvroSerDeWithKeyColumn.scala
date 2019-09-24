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

package za.co.absa.abris.avro

import java.security.InvalidParameterException

import org.apache.avro.Schema
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types.{ArrayType, BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}
import org.slf4j.LoggerFactory
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.schemas.SchemasProcessor
import za.co.absa.abris.avro.schemas.impl.{AvroToSparkProcessor, SparkToAvroProcessor}
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.{RETAIN_ORIGINAL_SCHEMA, RETAIN_SELECTED_COLUMN_ONLY, SchemaRetentionPolicy}
import za.co.absa.abris.avro.serde.{AvroDecoder, AvroToRowEncoderFactory}

/**
  * This object provides the main point of integration between applications and this library for data containing key and
  * value columns (e.g. Dataframes retrieved from Kafka).
  *
  * The APIs defined here are capable of working with two Avro schemas, one for the key and other for the value columns.
  *
  * Also, it is possible to retain the whole Dataframe structure after parsing back from Avro as well as only the key and value columns.
  * To shift between these options, it is STRONGLY recommended that users of this library understand the concept behind
  * [[za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies]].
  *
  * IMPORTANT: This API was created in a separate file since it was added later and the refactoring of the previous one [[za.co.absa.abris.avro.AvroSerDe]]
  * would be to big, thus, error-prone.
  */
object AvroSerDeWithKeyColumn {

  private val logger = LoggerFactory.getLogger(AvroSerDeWithKeyColumn.getClass)

  private def KEY_COLUMN_NAME   = "key"
  private def VALUE_COLUMN_NAME = "value"

  /**
    * This class provides the method that converts binary Avro records from a stream into Spark Rows on the fly.
    *
    * It loads binary data from a stream and feed them into an Avro/Spark decoder, returning the resulting rows.
    *
    * It requires the path to the Avro schema which defines the records to be read.
    */
  implicit class StreamDeserializer(dsReader: DataStreamReader) extends AvroDecoder {

    /**
      * Loads specific columns from the stream.
      */
    private def getStreamFromColumns(columns: String*) = dsReader.load.select(columns.head, columns.tail:_*)

    /**
      * Loads data from the full stream.
      */
    private def getFullStream() = dsReader.load

    /**
      * Gets the correct stream of data based on the retention policy.
      */
    private def getData(retentionPolicy: SchemaRetentionPolicy, columns: String*): DataFrame = {
      retentionPolicy match {
        case RETAIN_SELECTED_COLUMN_ONLY => getStreamFromColumns(KEY_COLUMN_NAME,VALUE_COLUMN_NAME)
        case RETAIN_ORIGINAL_SCHEMA      => getFullStream()
        case _ => throw new IllegalArgumentException(s"Invalid Schema Retention Policy: $retentionPolicy")
      }
    }

    /**
      * Uses instantiated Avro [[org.apache.avro.Schema]]s for key and value.
      */
    def fromAvro(keySchema: Schema, valueSchema: Schema)(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      val data = getData(retentionPolicy, KEY_COLUMN_NAME,VALUE_COLUMN_NAME)
      fromAvroToRowRetainingStructure(data, KEY_COLUMN_NAME, keySchema, VALUE_COLUMN_NAME, valueSchema)
    }

    /**
      * Uses Avro [[org.apache.avro.Schema]]s stored in the local file system for both, key and value.
      */
    def fromAvro(keySchemaPath: String, valueSchemaPath: String)(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      val data = getData(retentionPolicy, KEY_COLUMN_NAME,VALUE_COLUMN_NAME)
      fromAvroToRowRetainingStructure(data, KEY_COLUMN_NAME, keySchemaPath, VALUE_COLUMN_NAME, valueSchemaPath)
    }

    /**
      * Loads the Avro schemas from Schema Registry for both, key and value.
      */
    def fromAvro(schemaRegistryConf: Map[String,String])(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      val data = getData(retentionPolicy, KEY_COLUMN_NAME,VALUE_COLUMN_NAME)
      fromAvroToRowWithKeysRetainingStructure(data, KEY_COLUMN_NAME, VALUE_COLUMN_NAME, schemaRegistryConf)
    }

    /**
      * Loads the Avro schemas from Schema Registry for both, key and value.
      *
      * This method supports schema changes from Schema Registry. However, the conversion between Avro records and Spark
      * rows relies on RowEncoders, which are defined before the job starts. Thus, although the schema changes are supported
      * while reading, they are not translated to RowEncoders, which could take to errors in the final data.
      *
      * Refer to the [[za.co.absa.abris.avro.read.confluent.ScalaConfluentKafkaAvroDeserializer]] deserialize() method documentation to better understand how this
      * operation is performed.
      */
    def fromConfluentAvro(confluentConf: Map[String,String])(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      val data = getData(retentionPolicy, KEY_COLUMN_NAME,VALUE_COLUMN_NAME)
      fromConfluentAvroToRowWithKeys(data, KEY_COLUMN_NAME, VALUE_COLUMN_NAME, confluentConf)
    }

    /**
      * Uses instantiated [[org.apache.avro.Schema]]s to perform the conversions for both, key and value.
      *
      * This method supports schema changes from Schema Registry. However, the conversion between Avro records and Spark
      * rows relies on RowEncoders, which are defined before the job starts. Thus, although the schema changes are supported
      * while reading, they are not translated to RowEncoders, which could take to errors in the final data.
      *
      * Refer to the [[za.co.absa.abris.avro.read.confluent.ScalaConfluentKafkaAvroDeserializer]] deserialize() method documentation to better understand how this
      * operation is performed.
      */
    def fromConfluentAvro(keySchema: Schema, valueSchema: Schema, confluentConf: Map[String,String])(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      val data = getData(retentionPolicy, KEY_COLUMN_NAME,VALUE_COLUMN_NAME)
      fromConfluentAvroToRowWithKeys(data, KEY_COLUMN_NAME, keySchema, VALUE_COLUMN_NAME, valueSchema, confluentConf)
    }

    /**
      * Loads the Avro schemas from the local file system for both, key and value.
      *
      * This method supports schema changes from Schema Registry. However, the conversion between Avro records and Spark
      * rows relies on RowEncoders, which are defined before the job starts. Thus, although the schema changes are supported
      * while reading, they are not translated to RowEncoders, which could take to errors in the final data.
      *
      * Refer to the [[za.co.absa.abris.avro.read.confluent.ScalaConfluentKafkaAvroDeserializer]] deserialize() method documentation to better understand how this
      * operation is performed.
      */
    def fromConfluentAvro(keySchemaPath: String, valueSchemaPath: String, confluentConf: Map[String,String])(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      val data = getData(retentionPolicy, KEY_COLUMN_NAME,VALUE_COLUMN_NAME)
      fromConfluentAvroToRowWithKeys(data, KEY_COLUMN_NAME, keySchemaPath, VALUE_COLUMN_NAME, valueSchemaPath, confluentConf)
    }
  }

  /**
    * This class provides the method that converts binary Avro records from a Dataframe into Spark Rows on the fly.
    *
    * It loads binary data from a stream and feed them into an Avro/Spark decoder, returning the resulting rows.
    *
    * It requires the path to the Avro schema which defines the records to be read.
    */
  implicit class DataframeDeserializer(dataframe: Dataset[Row]) extends AvroDecoder {

    /**
      * Uses instantiated Avro [[org.apache.avro.Schema]]s for key and value.
      */
    def fromAvro(keySchema: Schema, valueSchema: Schema)(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      val data = getData(retentionPolicy, KEY_COLUMN_NAME, VALUE_COLUMN_NAME)
      fromAvroToRowRetainingStructure(data, KEY_COLUMN_NAME, keySchema, VALUE_COLUMN_NAME, valueSchema)
    }

    /**
      * Uses Avro [[org.apache.avro.Schema]]s stored in the local file system for both, key and value.
      */
    def fromAvro(keySchemaPath: String, valueSchemaPath: String)(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      val data = getData(retentionPolicy, KEY_COLUMN_NAME, VALUE_COLUMN_NAME)
      fromAvroToRowRetainingStructure(data, KEY_COLUMN_NAME, keySchemaPath, VALUE_COLUMN_NAME, valueSchemaPath)
    }

    /**
      * Loads the Avro schemas from Schema Registry for both, key and value.
      */
    def fromAvro(schemaRegistryConf: Map[String, String])(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      val data = getData(retentionPolicy, KEY_COLUMN_NAME, VALUE_COLUMN_NAME)
      fromAvroToRowWithKeysRetainingStructure(data, KEY_COLUMN_NAME, VALUE_COLUMN_NAME, schemaRegistryConf)
    }

    /**
      * Loads the Avro schemas from Schema Registry for both, key and value.
      *
      * This method supports schema changes from Schema Registry. However, the conversion between Avro records and Spark
      * rows relies on RowEncoders, which are defined before the job starts. Thus, although the schema changes are supported
      * while reading, they are not translated to RowEncoders, which could take to errors in the final data.
      *
      * Refer to the [[za.co.absa.abris.avro.read.confluent.ScalaConfluentKafkaAvroDeserializer]] deserialize() method documentation to better understand how this
      * operation is performed.
      */
    def fromConfluentAvro(confluentConf: Map[String, String])(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      val data = getData(retentionPolicy, KEY_COLUMN_NAME, VALUE_COLUMN_NAME)
      fromConfluentAvroToRowWithKeys(data, KEY_COLUMN_NAME, VALUE_COLUMN_NAME, confluentConf)
    }

    /**
      * Uses instantiated [[org.apache.avro.Schema]]s to perform the conversions for both, key and value.
      *
      * This method supports schema changes from Schema Registry. However, the conversion between Avro records and Spark
      * rows relies on RowEncoders, which are defined before the job starts. Thus, although the schema changes are supported
      * while reading, they are not translated to RowEncoders, which could take to errors in the final data.
      *
      * Refer to the [[za.co.absa.abris.avro.read.confluent.ScalaConfluentKafkaAvroDeserializer]] deserialize() method documentation to better understand how this
      * operation is performed.
      */
    def fromConfluentAvro(keySchema: Schema, valueSchema: Schema, confluentConf: Map[String, String])(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      val data = getData(retentionPolicy, KEY_COLUMN_NAME, VALUE_COLUMN_NAME)
      fromConfluentAvroToRowWithKeys(data, KEY_COLUMN_NAME, keySchema, VALUE_COLUMN_NAME, valueSchema, confluentConf)
    }

    /**
      * Loads the Avro schemas from the local file system for both, key and value.
      *
      * This method supports schema changes from Schema Registry. However, the conversion between Avro records and Spark
      * rows relies on RowEncoders, which are defined before the job starts. Thus, although the schema changes are supported
      * while reading, they are not translated to RowEncoders, which could take to errors in the final data.
      *
      * Refer to the [[za.co.absa.abris.avro.read.confluent.ScalaConfluentKafkaAvroDeserializer]] deserialize() method documentation to better understand how this
      * operation is performed.
      */
    def fromConfluentAvro(keySchemaPath: String, valueSchemaPath: String, confluentConf: Map[String, String])(retentionPolicy: SchemaRetentionPolicy): Dataset[Row] = {
      val data = getData(retentionPolicy, KEY_COLUMN_NAME, VALUE_COLUMN_NAME)
      fromConfluentAvroToRowWithKeys(data, KEY_COLUMN_NAME, keySchemaPath, VALUE_COLUMN_NAME, valueSchemaPath, confluentConf)
    }

    /**
      * Gets the correct dataset based on the retention policy.
      */
    private def getData(retentionPolicy: SchemaRetentionPolicy, columns: String*): DataFrame = {
      retentionPolicy match {
        case RETAIN_SELECTED_COLUMN_ONLY => getDataFromColumns(KEY_COLUMN_NAME, VALUE_COLUMN_NAME)
        case RETAIN_ORIGINAL_SCHEMA => dataframe
        case _ => throw new IllegalArgumentException(s"Invalid Schema Retention Policy: $retentionPolicy")
      }
    }

    /**
      * Loads specific columns from the dataset.
      */
    private def getDataFromColumns(columns: String*) = dataframe.select(columns.head, columns.tail: _*)
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
      * Checks if the incoming Dataframe has a correct schema, i.e. if it has 'key' and 'value' columns.
      *
      * @throws InvalidParameterException in case the schema is not set.
      */
    @throws[InvalidParameterException]
    private def checkDataframeSchema(): Boolean = {
      if (dataframe.schema == null ||
          dataframe.schema.fields.length != 2 ||
         (dataframe.schema.fields(0).name.toLowerCase != KEY_COLUMN_NAME && dataframe.schema.fields(1).name.toLowerCase != VALUE_COLUMN_NAME)) {
        throw new InvalidParameterException("Dataframe does not have a valid schema. The schema should be composed of two fields, " +
          "'key' and 'value' where each of those represent a column with its own nested schema.")
      }
      true
    }

    /**
      * Retrieves the Spark schema used by keys from the Dataframe schema.
      */
    private def getKeySchemaFromDataframe(): StructType = getFieldAsStructTypeByName(KEY_COLUMN_NAME)

    /**
      * Retrieves the Spark schema used by payloads(values) from the Dataframe schema.
      */
    private def getValueSchemaFromDataframe(): StructType = getFieldAsStructTypeByName(VALUE_COLUMN_NAME)

    /**
      * Retrieves a Spark StructField from the input Dataframe from its index.
      */
    private def getFieldByIndex(targetField: Int) = dataframe.schema.fields(targetField)

    /**
      * Retrieves the index of a field from its name, from the input Dataframe.
      */
    private def getFieldIndex(name: String) = dataframe.schema.fields.toList.indexWhere(_.name.toLowerCase == name.toLowerCase)

    /**
      * Retrieves a given field from the input Dataframe as a Spark StructType.
      */
    private def getFieldAsStructTypeByName(name: String): StructType = {
      // the outermost field is "key/value", here we get its nested structure, i.e. the payload structure for either, key or value
      getFieldByIndex(getFieldIndex(name)).dataType.asInstanceOf[StructType]
    }

    /**
      * Tries to manage schema registration in case credentials to access Schema Registry are provided.
      *
      * This method either works or throws an InvalidParameterException.
      */
    @throws[InvalidParameterException]
    private def manageSchemaRegistration(topic: String, keySchema: Schema, valueSchema: Schema, schemaRegistryConf: Map[String,String]): (Int,Int) = {
      (manageKeySchemaRegistration(topic, keySchema, schemaRegistryConf), manageValueSchemaRegistration(topic, valueSchema, schemaRegistryConf))
    }

    /**
      * Tries to manage key schema registration.
      *
      * This method either works or throws an InvalidParameterException.
      */
    @throws[InvalidParameterException]
    private def manageKeySchemaRegistration(topic: String, keySchema: Schema, schemaRegistryConf: Map[String,String]): Int = {

      logger.info(s"Serializer.manageSchemaRegistration: Registering key schema for topic $topic .")
      val keySchemaId   = AvroSchemaUtils.registerIfCompatibleKeySchema(topic, keySchema, schemaRegistryConf)

      if (keySchemaId.isEmpty) {
        throw new InvalidParameterException(s"Key Schema could not be registered for topic '$topic'. Make sure that the Schema Registry " +
          s"is available, the parameters are correct and the schemas are compatible")
      }
      else {
        logger.info(s"Schemas successfully registered for key for topic '$topic' with id '{${keySchemaId.get}}'.")
      }

      keySchemaId.get
    }

    /**
      * Tries to manage value schema registration.
      *
      * This method either works or throws an InvalidParameterException.
      */
    @throws[InvalidParameterException]
    private def manageValueSchemaRegistration(topic: String, valueSchema: Schema, schemaRegistryConf: Map[String,String]): Int = {

      logger.info(s"Serializer.manageSchemaRegistration: Registering value schema for topic $topic .")
      val valueSchemaId = AvroSchemaUtils.registerIfCompatibleValueSchema(topic, valueSchema, schemaRegistryConf)

      if (valueSchemaId.isEmpty) {
        throw new InvalidParameterException(s"Value schema could not be registered for topic '$topic'. Make sure that the Schema Registry " +
          s"is available, the parameters are correct and the schemas are compatible")
      }
      else {
        logger.info(s"Schemas successfully registered for value for topic '$topic' with id '{${valueSchemaId.get}}'.")
      }

      valueSchemaId.get
    }

    /**
      * Converts from Dataset[Row(key,value)] into Dataset[Row(Array[Byte],Array[Byte])] containing Avro records. In other words, converts the keys and values into Avro records.
      *
      * Intended to be used when there IS NOT a Spark schema available in the Dataframe but there is an expected Avro schema.
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
    def toAvro(keySchemaPath: String, valueSchemaPath: String): Dataset[Row] = {
      toAvro(AvroSchemaUtils.load(keySchemaPath), AvroSchemaUtils.load(valueSchemaPath))
    }

    /**
      * Converts from Dataset[Row(key,value)] into Dataset[Row(String,Array[Byte])] containing Avro records. In other words, converts the keys to their String
      * representations and values into Avro records.
      *
      * Intended to be used when there IS NOT a Spark schema available in the Dataframe but there is an expected Avro schema for the value column.
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
    def toAvroWithPlainKey(valueSchemaPath: String): Dataset[Row] = {
      toAvroWithPlainKey(AvroSchemaUtils.load(valueSchemaPath))
    }

    /**
      * Converts from Dataset[Row(key,value)] into Dataset[Row(Array[Byte],Array[Byte])] containing Avro records. In other words, converts the keys and values into Avro records.
      *
      * Intended to be used when THERE IS a Spark schema present in the Dataframe from which the Avro schema will be translated.
      *
      * The API will infer the Avro schema from the incoming Dataframe. The inferred schema will receive the name and namespace informed as parameters.
      *
      * The API will throw in case the Dataframe does not have a schema.
      *
      * Differently than the other API, this one does not suffer from the schema changing issue, since the final Avro schema will be derived from the schema
      * already used by Spark.
      */
    def toAvro(keySchemaName: String, keySchemaNamespace: String, valueSchemaName: String, valueSchemaNamespace: String): Dataset[Row] = {
      checkDataframeSchema()
      toAvro(dataframe, new SparkToAvroProcessor(getKeySchemaFromDataframe(), keySchemaName, keySchemaNamespace),
        new SparkToAvroProcessor(getValueSchemaFromDataframe(), valueSchemaName, valueSchemaNamespace))(None, None)
    }

    /**
      * Converts from Dataset[Row(key,value)] into Dataset[Row(String,Array[Byte])] being the value an Avro record. In other words, converts the keys into strings and values into Avro records.
      *
      * Intended to be used when THERE IS a Spark schema present in the Dataframe from which the Avro schema will be translated.
      *
      * The API will infer the Avro schema from the value column in the incoming Dataframe. The inferred schema will receive the name and namespace informed as parameters.
      *
      * The API will throw in case the Dataframe does not have a schema.
      *
      * Differently than the other API, this one does not suffer from the schema changing issue, since the final Avro schema will be derived from the schema
      * already used by Spark.
      */
    def toAvroWithPlainKey(valueSchemaName: String, valueSchemaNamespace: String): Dataset[Row] = {
      checkDataframeSchema()
      toAvroWithPlainStringKey(dataframe, new SparkToAvroProcessor(getValueSchemaFromDataframe(), valueSchemaName, valueSchemaNamespace))(None, None)
    }

    /**
      * Converts from Dataset[Row(key,value)] into Dataset[Row(Array[Byte],Array[Byte])] containing Avro records. In other words, converts the keys and values into Avro records.
      *
      * Intended to be used when there IS NO Spark schema available in the Dataframe but there is an expected Avro schema.
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
    def toAvro(keySchema: Schema, valueSchema: Schema): Dataset[Row] = {
      toAvro(dataframe, new AvroToSparkProcessor(keySchema.toString), new AvroToSparkProcessor(valueSchema.toString))(None, None)
    }

    /**
      * Converts from Dataset[Row(key,value)] into Dataset[Row(String,Array[Byte])] containing Avro records. In other words, converts the keys to their String
      * representations and values into Avro records.
      *
      * Intended to be used when there IS NO Spark schema available in the Dataframe but there is an expected Avro schema.
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
    def toAvroWithPlainKey(valueSchema: Schema): Dataset[Row] = {
      toAvroWithPlainStringKey(dataframe, new AvroToSparkProcessor(valueSchema.toString))(None, None)
    }

    /**
      * Converts from Dataset[Row(key,value)] into Dataset[Row(Array[Byte],Array[Byte])] containing Avro records. In other words, converts the keys and values into Avro records.
      *
      * Intended to be used when THERE IS a Spark schema present in the Dataframe from which the Avro schema will be translated.
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
    def toAvro(topic: String, keySchemaName: String, keySchemaNamespace: String, valueSchemaName: String, valueSchemaNamespace: String)(schemaRegistryConf: Option[Map[String,String]] = None): Dataset[Row] = {

      checkDataframeSchema()

      val keySchemaProcessor   = new SparkToAvroProcessor(getKeySchemaFromDataframe(), keySchemaName, keySchemaNamespace)
      val valueSchemaProcessor = new SparkToAvroProcessor(getValueSchemaFromDataframe(), valueSchemaName, valueSchemaNamespace)

      if (schemaRegistryConf.isDefined) {
        manageSchemaRegistration(topic, keySchemaProcessor.getAvroSchema(), valueSchemaProcessor.getAvroSchema(), schemaRegistryConf.get)
      }

      toAvro(dataframe, keySchemaProcessor, valueSchemaProcessor)(None, None)
    }

    /**
      * Converts from Dataset[Row(key,value)] into Dataset[Row(String,Array[Byte])] containing Avro records. In other words, converts the keys to their String
      * representations and values into Avro records.
      *
      * Intended to be used when THERE IS a Spark schema present in the Dataframe from which the Avro schema will be translated.
      *
      * The API will infer the Avro schema from the "value" column from the incoming Dataframe. The inferred schema will receive the name and namespace informed as parameters.
      *
      * The inferred schema will be registered with Schema Registry in case the parameters for accessing it are provided.
      *
      * The API will throw in case the Dataframe does not have a schema or if Schema Registry access details are provided but the schema could not be registered due to
      * either incompatibility, wrong credentials or Schema Registry unavailability.
      *
      * Differently than the other API, this one does not suffer from the schema changing issue, since the final Avro schema will be derived from the schema
      * already used by Spark.
      */
    def toAvroWithPlainKey(topic: String, valueSchemaName: String, valueSchemaNamespace: String)(schemaRegistryConf: Option[Map[String,String]] = None): Dataset[Row] = {

      checkDataframeSchema()

      val valueSchemaProcessor = new SparkToAvroProcessor(getValueSchemaFromDataframe(), valueSchemaName, valueSchemaNamespace)

      if (schemaRegistryConf.isDefined) {
        manageValueSchemaRegistration(topic, valueSchemaProcessor.getAvroSchema(), schemaRegistryConf.get)
      }

      toAvroWithPlainStringKey(dataframe, valueSchemaProcessor)(None, None)
    }

    /**
      * Converts from Dataset[Row] into Dataset[Array[Byte]] containing Avro records with the id of the schema in Schema Registry attached to the beginning of the payload.
      *
      * Intended to be used when THERE IS a Spark schema present in the Dataframe from which the Avro schema will be translated.
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
    def toConfluentAvro(topic: String, keySchemaName: String, keySchemaNamespace: String, valueSchemaName: String, valueSchemaNamespace: String)(schemaRegistryConf: Map[String,String]): Dataset[Row] = {

      if (schemaRegistryConf.isEmpty) {
        throw new IllegalArgumentException("No Schema Registry connection parameters found. It is mandatory for this API entry to connect to Schema Registry so that" +
          " the inferred schema can be registered or updated and its id can be retrieved to be sent along with the payload.")
      }

      checkDataframeSchema()

      val keySchemaProcessor   = new SparkToAvroProcessor(getKeySchemaFromDataframe(), keySchemaName, keySchemaNamespace)
      val valueSchemaProcessor = new SparkToAvroProcessor(getValueSchemaFromDataframe(), valueSchemaName, valueSchemaNamespace)

      val schemasIds = manageSchemaRegistration(topic, keySchemaProcessor.getAvroSchema(), valueSchemaProcessor.getAvroSchema(), schemaRegistryConf)

      toAvro(dataframe, keySchemaProcessor, valueSchemaProcessor)(Some(schemasIds._1), Some(schemasIds._2))
    }

    /**
      * Converts from Dataset[Row(key,value)] into Dataset[Row(String,Array[Byte])] containing Avro records. In other words, converts the keys to their String
      * representations and values into Avro records.
      *
      * Intended to be used when THERE IS a Spark schema present in the "value" column in the Dataframe from which the Avro schema will be translated.
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
    def toConfluentAvroWithPlainKey(topic: String, valueSchemaName: String, valueSchemaNamespace: String)(schemaRegistryConf: Map[String,String]): Dataset[Row] = {

      if (schemaRegistryConf.isEmpty) {
        throw new IllegalArgumentException("No Schema Registry connection parameters found. It is mandatory for this API entry to connect to Schema Registry so that" +
          " the inferred schema can be registered or updated and its id can be retrieved to be sent along with the payload.")
      }

      checkDataframeSchema()

      val valueSchemaProcessor = new SparkToAvroProcessor(getValueSchemaFromDataframe(), valueSchemaName, valueSchemaNamespace)

      val valueSchemaId = manageValueSchemaRegistration(topic, valueSchemaProcessor.getAvroSchema(), schemaRegistryConf)

      toAvroWithPlainStringKey(dataframe, valueSchemaProcessor)(None, Some(valueSchemaId))
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
    def toConfluentAvro(topic: String, keySchemaPath: String, valueSchemaPath: String)(schemaRegistryConf: Map[String,String]): Dataset[Row] = {

      if (schemaRegistryConf.isEmpty) {
        throw new IllegalArgumentException("No Schema Registry connection parameters found. It is mandatory for this API entry to connect to Schema Registry so that" +
          " the inferred schema can be registered or updated and its id can be retrieved to be sent along with the payload.")
      }

      val keySchema   = AvroSchemaUtils.load(keySchemaPath)
      val valueSchema = AvroSchemaUtils.load(valueSchemaPath)

      val keySchemaProcessor   = new AvroToSparkProcessor(keySchema.toString)
      val valueSchemaProcessor = new AvroToSparkProcessor(valueSchema.toString)

      val schemasIds = manageSchemaRegistration(topic, keySchemaProcessor.getAvroSchema(), valueSchemaProcessor.getAvroSchema(), schemaRegistryConf)

      toAvro(dataframe, keySchemaProcessor, valueSchemaProcessor)(Some(schemasIds._1), Some(schemasIds._2))
    }

    /**
      * Converts from Dataset[Row(key,value)] into Dataset[Row(String,Array[Byte])] containing Avro records. In other words, converts the keys to their String
      * representations and values into Avro records.
      *
      * Intended to be used when users want to force an Avro schema on the Dataframe, instead of having it inferred.
      *
      * The informed schema will be registered with Schema Registry in case the parameters for accessing it are provided an it is compatible with the currently registered version.
      *
      * The API will throw if Schema Registry access details are provided but the schema could not be registered due to either incompatibility, wrong credentials or Schema Registry unavailability.
      */
    def toConfluentAvroWithPlainKey(topic: String, valueSchemaPath: String)(schemaRegistryConf: Map[String,String]): Dataset[Row] = {

      if (schemaRegistryConf.isEmpty) {
        throw new IllegalArgumentException("No Schema Registry connection parameters found. It is mandatory for this API entry to connect to Schema Registry so that" +
          " the inferred schema can be registered or updated and its id can be retrieved to be sent along with the payload.")
      }

      val valueSchema = AvroSchemaUtils.load(valueSchemaPath)

      val valueSchemaProcessor = new AvroToSparkProcessor(valueSchema.toString)

      val valueSchemaId = manageValueSchemaRegistration(topic, valueSchemaProcessor.getAvroSchema(), schemaRegistryConf)

      toAvroWithPlainStringKey(dataframe, valueSchemaProcessor)(None, Some(valueSchemaId))
    }

    /**
      * Converts a Dataset[Row] into a Dataset[Array[Byte]] containing Avro schemas generated according to the plain specification informed as a parameter.
      */
    private def toAvro(rows: Dataset[Row], keySchemas: SchemasProcessor, valueSchemas: SchemasProcessor)(keySchemaId: Option[Int], valueSchemaId: Option[Int]) = {

      val resultingRowSchema = StructType(List(StructField(KEY_COLUMN_NAME, BinaryType, false),StructField(VALUE_COLUMN_NAME, BinaryType, false)))

      implicit val recEncoder: Encoder[Row] = AvroToRowEncoderFactory.createRowEncoder(resultingRowSchema)

      rows
        .mapPartitions(partition => {

        val keyAvroSchema    = keySchemas.getAvroSchema()
        val keySparkSchema   = keySchemas.getSparkSchema()
        val valueAvroSchema  = valueSchemas.getAvroSchema()
        val valueSparkSchema = valueSchemas.getSparkSchema()

        partition.map(row =>
        {
          val key   = row.get(0).asInstanceOf[Row]
          val value = row.get(1).asInstanceOf[Row]

          val binaryKeyRecord   = SparkAvroConversions.rowToBinaryAvro(key, keySparkSchema, keyAvroSchema, keySchemaId)
          val binaryValueRecord = SparkAvroConversions.rowToBinaryAvro(value, valueSparkSchema, valueAvroSchema, valueSchemaId)

          new GenericRowWithSchema(Array(binaryKeyRecord, binaryValueRecord), resultingRowSchema).asInstanceOf[Row]
        }
        )
      })
    }

    /**
      * Converts a Dataset[Row] into a Dataset[Array[Byte]] containing Avro schemas generated according to the plain specification informed as a parameter.
      *
      * This method converts the key into a string by invoking .toString()
      */
    private def toAvroWithPlainStringKey(rows: Dataset[Row], valueSchemas: SchemasProcessor)(keySchemaId: Option[Int], valueSchemaId: Option[Int]) = {
      val includeHeader = rows.toDF().schema.exists(_.name == "headers")
      val resultingRowSchema =
        if (includeHeader)
          StructType(List(StructField(KEY_COLUMN_NAME, StringType, false),StructField(VALUE_COLUMN_NAME, BinaryType, false), StructField("headers", ArrayType(StructType(Array(
            StructField("key", StringType),
            StructField("value", BinaryType)))), false)))
        else
          StructType(List(StructField(KEY_COLUMN_NAME, StringType, false),StructField(VALUE_COLUMN_NAME, BinaryType, false)))

      implicit val recEncoder: Encoder[Row] = AvroToRowEncoderFactory.createRowEncoder(resultingRowSchema)

      rows
        .mapPartitions(partition => {

          val valueAvroSchema  = valueSchemas.getAvroSchema()
          val valueSparkSchema = valueSchemas.getSparkSchema()

          partition.map(row =>
          {
            val key   = row.get(0).toString
            val value = row.get(1).asInstanceOf[Row]

            val binaryValueRecord = SparkAvroConversions.rowToBinaryAvro(value, valueSparkSchema, valueAvroSchema, valueSchemaId)
            if (includeHeader)
              new GenericRowWithSchema(Array(key, binaryValueRecord, row.get(2)), resultingRowSchema).asInstanceOf[Row]
            else
              new GenericRowWithSchema(Array(key, binaryValueRecord), resultingRowSchema).asInstanceOf[Row]
          }
          )
        })
    }
  }
}