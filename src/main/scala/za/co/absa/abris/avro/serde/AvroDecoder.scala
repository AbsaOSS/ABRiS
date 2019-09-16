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

package za.co.absa.abris.avro.serde

import java.security.InvalidParameterException

import org.apache.avro.Schema
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Dataset, Encoders, Row}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils

/**
  * Converts binary Avro records into Spark Rows.
  */
private[avro] class AvroDecoder {

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
  protected def fromConfluentAvroToRow(dataframe: Dataset[Row], schemaPath: Option[String], schemaRegistryConf: Option[Map[String,String]]): Dataset[Row] = {

    if (schemaPath.isEmpty && schemaRegistryConf.isEmpty) {
      throw new InvalidParameterException("Neither path to schema in file system nor Schema Registry configurations was provided.")
    }

    implicit val rowEncoder = if (schemaRegistryConf.isDefined) {
      AvroToRowEncoderFactory.createRowEncoder(schemaRegistryConf.get)
    }
    else {
      AvroToRowEncoderFactory.createRowEncoder(schemaPath.get)
    }

    dataframe
      .as(Encoders.BINARY)
      .mapPartitions(partition => {
        val avroReader = AvroReaderFactory.createConfiguredConfluentAvroReader(schemaPath, schemaRegistryConf)
        val avroToRowConverter = new AvroToRowConverter(None)
        partition.map(avroRecord => {
          avroToRowConverter.convert(avroReader.deserialize(avroRecord))
        })
      })
  }

  protected def fromConfluentAvroToRowWithKeys(dataframe: Dataset[Row], keyColName: String, valueColName: String, schemaRegistryConf: Map[String,String]): Dataset[Row] = {

    if (schemaRegistryConf.isEmpty) {
      throw new InvalidParameterException("Schema Registry configurations is required.")
    }

    val valueSchema = AvroSchemaUtils.loadForValue(schemaRegistryConf)
    val keySchema = AvroSchemaUtils.loadForKey(schemaRegistryConf)

    fromConfluentAvroToRowWithKeys(dataframe, keyColName, keySchema, valueColName, valueSchema, schemaRegistryConf)
  }

  protected def fromConfluentAvroToRowWithKeys(dataframe: Dataset[Row], keyColName: String, keySchemaPath: String, valueColName: String, valueSchemaPath: String, schemaRegistryConf: Map[String,String]): Dataset[Row] = {

    val keySchema = AvroSchemaUtils.load(keySchemaPath)
    val valueSchema = AvroSchemaUtils.load(valueSchemaPath)

    fromConfluentAvroToRowWithKeys(dataframe, keyColName, keySchema, valueColName, valueSchema, schemaRegistryConf)
  }

  protected def fromConfluentAvroToRowWithKeys(dataframe: Dataset[Row], keyColName: String, keyColSchema: Schema, valueColName: String, valueColSchema: Schema, schemaRegistryConf: Map[String,String]): Dataset[Row] = {

    if (schemaRegistryConf.isEmpty) {
      throw new InvalidParameterException("Schema Registry configurations is required.")
    }

    val originalSchema = dataframe.schema

    // gets the index, from Dataframe's Spark schema, of the key and value columns
    val keyColIndex   = originalSchema.fields.toList.indexWhere(_.name.toLowerCase == keyColName.toLowerCase)
    val valueColIndex = originalSchema.fields.toList.indexWhere(_.name.toLowerCase == valueColName.toLowerCase)

    // updates the schemas for the key and value columns
    originalSchema.fields(keyColIndex)   = new StructField(keyColName, SparkAvroConversions.toSqlType(keyColSchema), false)
    originalSchema.fields(valueColIndex) = new StructField(valueColName, SparkAvroConversions.toSqlType(valueColSchema), false)

    // creates a row encoder for the updated schema
    implicit val rowEncoder = AvroToRowEncoderFactory.createRowEncoder(originalSchema)

    // gets key and value columns schemas as plain text so that they can be serialized
    val keyColPlainSchema   = keyColSchema.toString
    val valueColPlainSchema = valueColSchema.toString

    dataframe
      .mapPartitions(partition => {

        val keyAvroReader = AvroReaderFactory.createConfiguredConfluentAvroReader(AvroSchemaUtils.parse(keyColPlainSchema), schemaRegistryConf)
        val valueAvroReader = AvroReaderFactory.createConfiguredConfluentAvroReader(AvroSchemaUtils.parse(valueColPlainSchema), schemaRegistryConf)

        val avroToRowConverter = new AvroToRowConverter(None)

        partition.map(avroRecord => {

          val keySparkType   = avroToRowConverter.convert(keyAvroReader.deserialize(avroRecord.get(keyColIndex).asInstanceOf[Array[Byte]]))
          val valueSparkType = avroToRowConverter.convert(valueAvroReader.deserialize(avroRecord.get(valueColIndex).asInstanceOf[Array[Byte]]))

          val array: Array[Any] = new Array(avroRecord.size)

          for (i <- 0 until avroRecord.size) {
            array(i) = avroRecord.get(i)
          }
          array(keyColIndex)   = keySparkType
          array(valueColIndex) = valueSparkType

          Row.fromSeq(array)
        })
      })
  }

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
    *
    * 3. The extracted Avro record will be put inside the destination column, as a nested structure.
    */
  protected def fromConfluentAvroToRow(dataframe: Dataset[Row], schemaPath: Option[String], schemaRegistryConf: Option[Map[String,String]], destinationColumn: String): Dataset[Row] = {

    if (schemaPath.isEmpty && schemaRegistryConf.isEmpty) {
      throw new InvalidParameterException("Neither path to schema in file system nor Schema Registry configurations was provided.")
    }

    val dataSchema = if (schemaRegistryConf.isDefined) {
      AvroSchemaUtils.loadForValue(schemaRegistryConf.get)
    }
    else {
      AvroSchemaUtils.load(schemaPath.get)
    }

    val originalSchema = dataframe.schema

    // sets the Avro schema into the destination field
    val destinationIndex = originalSchema.fields.toList.indexWhere(_.name.toLowerCase == destinationColumn.toLowerCase)
    originalSchema.fields(destinationIndex) = new StructField(destinationColumn, SparkAvroConversions.toSqlType(dataSchema), false)

    implicit val rowEncoder = AvroToRowEncoderFactory.createRowEncoder(originalSchema)

    dataframe
      .mapPartitions(partition => {

        val avroReader = AvroReaderFactory.createConfiguredConfluentAvroReader(schemaPath, schemaRegistryConf)
        val avroToRowConverter = new AvroToRowConverter(None)

        partition.map(avroRecord => {

          val sparkType = avroToRowConverter.convert(avroReader.deserialize(avroRecord.get(destinationIndex).asInstanceOf[Array[Byte]]))
          val array: Array[Any] = new Array(avroRecord.size)

          for (i <- 0 until avroRecord.size) {
            array(i) = avroRecord.get(i)
          }
          array(destinationIndex) = sparkType
          Row.fromSeq(array)
        })
      })
  }

  /**
    * Converts the binary Avro records contained in the Dataframe into regular Rows with a
    * SQL schema whose specification is translated from the Avro schema informed.
    *
    * Stores the decoded Avro record into 'destinationColumn' while maintaining the schema present in the dataframe.
    */
  protected def fromAvroToRow(dataframe: Dataset[Row], schema: Schema, destinationColumn: String): Dataset[Row] = {

    val originalSchema = dataframe.schema

    // sets the Avro schema into the destination field
    val destinationIndex = originalSchema.fields.toList.indexWhere(_.name.toLowerCase == destinationColumn.toLowerCase)
    originalSchema.fields(destinationIndex) = new StructField(destinationColumn, SparkAvroConversions.toSqlType(schema), false)

    // creates the row encoder for the whole schema
    implicit val rowEncoder = AvroToRowEncoderFactory.createRowEncoder(originalSchema)

    val plainSchema = schema.toString

    dataframe
      .mapPartitions(partition => {

        val avroToRowConverter = new AvroToRowConverter(Some(AvroReaderFactory.createAvroReader(AvroSchemaUtils.parse(plainSchema))))

        partition.map(avroRecord => {

          val sparkType = avroToRowConverter.convert(avroRecord.get(destinationIndex).asInstanceOf[Array[Byte]])
          val array: Array[Any] = new Array(avroRecord.size)

          for (i <- 0 until avroRecord.size) {
            array(i) = avroRecord.get(i)
          }
          array(destinationIndex) = sparkType
          Row.fromSeq(array)
        })
      })
  }

  /**
    * Converts the binary Avro records contained in specified columns of the Dataframe into regular Rows with a
    * SQL schema whose specification is translated from the Avro schema informed.
    *
    * Apart from the columns that will be changed to hold the new Spark schemas translated from the Avro ones, the
    * rest of the schema for the Dataset will remain unchanged.
    */
  protected def fromAvroToRowRetainingStructure(dataframe: Dataset[Row], keyColName: String, keyColSchema: Schema, valueColName: String, valueColSchema: Schema): Dataset[Row] = {

    val originalSchema = dataframe.schema

    // gets the index, from Dataframe's Spark schema, of the key and value columns
    val keyColIndex   = originalSchema.fields.toList.indexWhere(_.name.toLowerCase == keyColName.toLowerCase)
    val valueColIndex = originalSchema.fields.toList.indexWhere(_.name.toLowerCase == valueColName.toLowerCase)

    // updates the schemas for the key and value columns
    originalSchema.fields(keyColIndex)   = new StructField(keyColName, SparkAvroConversions.toSqlType(keyColSchema), false)
    originalSchema.fields(valueColIndex) = new StructField(valueColName, SparkAvroConversions.toSqlType(valueColSchema), false)

    // creates a row encoder for the updated schema
    implicit val rowEncoder = AvroToRowEncoderFactory.createRowEncoder(originalSchema)

    // gets key and value columns schemas as plain text so that they can be serialized
    val keyColPlainSchema   = keyColSchema.toString
    val valueColPlainSchema = valueColSchema.toString

    dataframe
      .mapPartitions(partition => {

        // creates converters for the key and value Avro records contained in the Dataframe columns
        val keyColConverter  = new AvroToRowConverter(Some(AvroReaderFactory.createAvroReader(AvroSchemaUtils.parse(keyColPlainSchema))))
        val valueColConverter = new AvroToRowConverter(Some(AvroReaderFactory.createAvroReader(AvroSchemaUtils.parse(valueColPlainSchema))))

        partition.map(avroRecords => {

          // gets the binary Avro data from the key and value columns
          val keyRecord   = avroRecords.get(avroRecords.fieldIndex(keyColName)).asInstanceOf[Array[Byte]]
          val valueRecord = avroRecords.get(avroRecords.fieldIndex(valueColName)).asInstanceOf[Array[Byte]]

          // converts the Avro records into Spark Rows
          val keySparkType   = keyColConverter.convert(avroRecords.get(keyColIndex).asInstanceOf[Array[Byte]])
          val valueSparkType = valueColConverter.convert(avroRecords.get(valueColIndex).asInstanceOf[Array[Byte]])

          // copies all the fields to the new column set
          val columns: Array[Any] = new Array(avroRecords.size)
          for (i <- 0 until avroRecords.size) {
            columns(i) = avroRecords.get(i)
          }

          // updates the key and value columns values
          columns(keyColIndex)   = keySparkType
          columns(valueColIndex) = valueSparkType

          Row.fromSeq(columns)
        })
      })
  }

  /**
    * Converts the binary Avro records contained in the Dataframe into regular Rows with a
    * SQL schema whose specification is translated from the Avro schema informed.
    */
  protected def fromAvroToRow(dataframe: Dataset[Row], schema: Schema): Dataset[Row] = {

    implicit val rowEncoder = AvroToRowEncoderFactory.createRowEncoder(schema)

    // transforming to plain and reparsing inside mapping because Schema instances are not serializable.
    val plainSchema = schema.toString

    dataframe
      .as(Encoders.BINARY)
      .mapPartitions(partition => {
        val avroToRowConverter = new AvroToRowConverter(Some(AvroReaderFactory.createAvroReader(AvroSchemaUtils.parse(plainSchema))))
        partition.map(avroRecord => {
          avroToRowConverter.convert(avroRecord)
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
  protected def fromAvroToRow(dataframe: Dataset[Row], schemaPath: String, destinationColumn: String): Dataset[Row] = {
    fromAvroToRow(dataframe, AvroSchemaUtils.load(schemaPath), destinationColumn)
  }

  /**
    * Converts the binary Avro records contained in the Dataframe into regular Rows with a
    * SQL schema whose specification is translated from the Avro schema informed.
    */
  protected def fromAvroToRowRetainingStructure(dataframe: Dataset[Row], keyColName: String, keySchemaPath: String, valueColName: String, valueSchemaPath: String): Dataset[Row] = {
    fromAvroToRowRetainingStructure(dataframe, keyColName, AvroSchemaUtils.load(keySchemaPath), valueColName, AvroSchemaUtils.load(valueSchemaPath))
  }

  /**
    * Converts the binary Avro records contained in the Dataframe into regular Rows with a
    * SQL schema whose specification is translated from the Avro schema informed.
    */
  protected def fromAvroToRow(dataframe: Dataset[Row], schemaRegistryConf: Map[String,String]): Dataset[Row] = {

    val schema = AvroSchemaUtils.loadForValue(schemaRegistryConf)
    implicit val rowEncoder: ExpressionEncoder[Row] = AvroToRowEncoderFactory.createRowEncoder(schema)

    // has to convert into String and re-parse it inside the 'map' operation since Avro Schema instances are not serializable
    val plainSchema = schema.toString()

    dataframe
      .as(Encoders.BINARY)
      .mapPartitions(partition => {
        val avroDecoder = new AvroToRowConverter(Some(AvroReaderFactory.createAvroReader(AvroSchemaUtils.parse(plainSchema))))
        partition.map(avroRecord => {
          avroDecoder.convert(avroRecord)
        })
      })
  }

  /**
    * Converts the binary Avro records contained in the Dataframe into regular Rows with a
    * SQL schema whose specification is translated from the Avro schema informed.
    */
  protected def fromAvroToRowWithKeysRetainingStructure(dataframe: Dataset[Row], keyColName: String, valueColName: String, schemaRegistryConf: Map[String,String]): Dataset[Row] = {
    val valueSchema = AvroSchemaUtils.loadForValue(schemaRegistryConf)
    val keySchema = AvroSchemaUtils.loadForKey(schemaRegistryConf)
    fromAvroToRowRetainingStructure(dataframe, keyColName, keySchema, valueColName, valueSchema)
  }

  /**
    * Converts the binary Avro records contained in the Dataframe into regular Rows with a
    * SQL schema whose specification is translated from the Avro schema informed.
    *
    * The Avro record will be stored inside ''destinationColumn'' as a nested structure.
    */
  protected def fromAvroToRow(dataframe: Dataset[Row], schemaRegistryConf: Map[String,String], destinationColumn: String): Dataset[Row] = {

    val schema = AvroSchemaUtils.loadForValue(schemaRegistryConf)

    val originalSchema = dataframe.schema

    // sets the Avro schema into the destination field
    val destinationIndex = originalSchema.fields.toList.indexWhere(_.name.toLowerCase == destinationColumn.toLowerCase)
    originalSchema.fields(destinationIndex) = new StructField(destinationColumn, SparkAvroConversions.toSqlType(schema), false)

    // creates the row encoder for the whole schema
    implicit val rowEncoder = AvroToRowEncoderFactory.createRowEncoder(originalSchema)

    // has to convert into String and re-parse it inside the 'map' operation since Avro Schema instances are not serializable
    val plainSchema = schema.toString()

    dataframe
      //.as(Encoders.BINARY)
      .mapPartitions(partition => {

        val avroToRowConverter = new AvroToRowConverter(Some(AvroReaderFactory.createAvroReader(AvroSchemaUtils.parse(plainSchema))))

        partition.map(avroRecord => {

          val sparkType = avroToRowConverter.convert(avroRecord.get(destinationIndex).asInstanceOf[Array[Byte]])
          val array: Array[Any] = new Array(avroRecord.size)

          for (i <- 0 until avroRecord.size) {
            array(i) = avroRecord.get(i)
          }
          array(destinationIndex) = sparkType
          Row.fromSeq(array)
        })
      })
  }
}
