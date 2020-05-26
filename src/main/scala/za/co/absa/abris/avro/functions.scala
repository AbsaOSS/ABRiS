/*
 * Copyright 2019 ABSA Group Limited
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

import org.apache.spark.sql.Column
import za.co.absa.abris.avro.read.confluent._
import za.co.absa.abris.avro.schemas.RegistryConfig
import za.co.absa.abris.avro.sql.{AvroDataToCatalyst, CatalystDataToAvro, SchemaProvider}


// scalastyle:off: object.name
object functions {
// scalastyle:on: object.name
// scalastyle:off: method.name

  /**
   * Converts a binary column of avro format into its corresponding catalyst value. The specified
   * schema must match the read data, otherwise the behavior is undefined: it may fail or return
   * arbitrary result.
   *
   * @param data the binary column.
   * @param jsonFormatSchema the avro schema in JSON string format.
   *
   */
  def from_avro(data: Column, jsonFormatSchema: String): Column = {
    new Column(AvroDataToCatalyst(
      data.expr,
      jsonFormatSchema,
      None,
      confluentCompliant = false
    ))
  }

  /**
   * Converts a binary column of avro format into its corresponding catalyst value using schema registry.
   * The schema loaded from schema registry must match the read data, otherwise the behavior is undefined:
   * it may fail or return arbitrary result.
   *
   * @param data the binary column.
   * @param schemaRegistryConf schema registry configuration.
   *
   */
  def from_avro(
    data: Column,
    schemaRegistryConf: Map[String,String]
  ): Column = {
    val schema = SchemaManagerFactory.create(schemaRegistryConf).downloadSchema()

    new Column(AvroDataToCatalyst(
      data.expr,
      schema.toString(),
      Some(schemaRegistryConf),
      confluentCompliant = false
    ))
  }

  /**
   * Converts a binary column of confluent avro format into its corresponding catalyst value using schema registry.
   * There are two avro schemas used: writer schema and reader schema.
   *
   * The configuration you provide (naming strategy, topic, ...) is used for getting the reader schema from schema
   * registry. The writer schema is also loaded from the registry but it's found by the schema id that is taken from
   * beginning of confluent avro payload.
   *
   * @param data the binary column.
   * @param schemaRegistryConf schema registry configuration.
   *
   */
  def from_confluent_avro(
    data: Column,
    schemaRegistryConf: Map[String,String]
  ): Column = {
    val schema = SchemaManagerFactory.create(schemaRegistryConf).downloadSchema()

    new Column(AvroDataToCatalyst(
      data.expr,
      schema.toString(),
      Some(schemaRegistryConf),
      confluentCompliant = true
    ))
  }

  /**
   * Converts a binary column of confluent avro format into its corresponding catalyst value using schema registry.
   * There are two avro schemas used: writer schema and reader schema.
   *
   * The reader schema is provided as a parameter.
   *
   * The writer schema is loaded from the registry, it's found by the schema id that is taken from
   * beginning of confluent avro payload.
   *
   * @param data the binary column.
   * @param readerSchema the reader avro schema in JSON string format.
   * @param schemaRegistryConf schema registry configuration for getting the writer schema.
   *
   */
  def from_confluent_avro(
    data: Column,
    readerSchema: String,
    schemaRegistryConf: Map[String,String]
  ): Column = {
    new Column(AvroDataToCatalyst(
      data.expr,
      readerSchema,
      Some(schemaRegistryConf),
      confluentCompliant = true
    ))
  }

  /**
   * Converts a column into binary of avro format.
   * Schema is generated automatically.
   *
   */
  def to_avro(data: Column): Column = {
    new Column(CatalystDataToAvro(
      data.expr,
      SchemaProvider(),
      None,
      confluentCompliant = false
    ))
  }

  /**
   * Converts a column into binary of avro format.
   *
   * @param data column to be converted to avro
   * @param jsonFormatSchema schema used for conversion
   */
  def to_avro(data: Column, jsonFormatSchema: String): Column = {
    new Column(CatalystDataToAvro(
      data.expr,
      SchemaProvider(jsonFormatSchema),
      None,
      confluentCompliant = false
    ))
  }

  /**
   * Converts a column into binary of avro format
   *
   * If you provide schema Id or Version the schema is downloaded.
   * Otherwise Schema is generated automatically from spark catalyst data type and stored in schema registry.
   *
   * @param data column to be converted to avro
   * @param schemaRegistryConf schema registry configuration
   */
  def to_avro(
    data: Column,
    schemaRegistryConf: Map[String,String]
  ): Column = {

    val schemaProvider: SchemaProvider = createSchemaProvider(schemaRegistryConf)

    new Column(CatalystDataToAvro(
      data.expr, schemaProvider, Some(schemaRegistryConf), confluentCompliant = false))
  }

  /**
   * Converts a column into binary of avro format and store the used schema in schema registry
   *
   * @param data column to be converted to avro
   * @param jsonFormatSchema schema used for conversion
   * @param schemaRegistryConf schema registry configuration
   */
  def to_avro(
    data: Column,
    jsonFormatSchema: String,
    schemaRegistryConf: Map[String,String]
  ): Column = {
    new Column(CatalystDataToAvro(
      data.expr,
      SchemaProvider(jsonFormatSchema),
      Some(schemaRegistryConf),
      confluentCompliant = false
    ))
  }

  /**
   * Converts a column into binary of avro format and prepend the schema id to avro payload
   * (according to confluent avro format)
   *
   * If you provide schema Id or Version the schema is downloaded and it's id is prepended.
   * Otherwise Schema is generated automatically from spark catalyst data type and stored in schema registry.
   *
   * @param data column to be converted to avro
   * @param schemaRegistryConf schema registry configuration
   */
  def to_confluent_avro(
    data: Column,
    schemaRegistryConf: Map[String,String]
  ): Column = {

    val schemaProvider: SchemaProvider = createSchemaProvider(schemaRegistryConf)

    new Column(CatalystDataToAvro(
      data.expr, schemaProvider, Some(schemaRegistryConf), confluentCompliant = true))
  }

  /**
   * Converts a column into binary of avro format, store the used schema in schema registry and prepend the schema id
   * to avro payload (according to confluent avro format)
   *
   * @param data column to be converted to avro
   * @param schemaRegistryConf schema registry configuration
   */
  def to_confluent_avro(
    data: Column,
    jsonFormatSchema: String,
    schemaRegistryConf: Map[String,String]
   ): Column = {

    new Column(CatalystDataToAvro(
      data.expr,
      SchemaProvider(jsonFormatSchema),
      Some(schemaRegistryConf),
      confluentCompliant = true
    ))
  }


  private def createSchemaProvider(schemaRegistryConf: Map[String, String]) = {
    val registryConfig = new RegistryConfig(schemaRegistryConf)

    if (registryConfig.isIdOrVersionDefined) {
      val schemaManager = SchemaManagerFactory.create(schemaRegistryConf)
      val schema = schemaManager.downloadSchema()
      SchemaProvider(schema.toString)
    } else {
      SchemaProvider(registryConfig.schemaNameOption, registryConfig.schemaNameSpaceOption)
    }
  }

  // scalastyle:on: method.name
}
