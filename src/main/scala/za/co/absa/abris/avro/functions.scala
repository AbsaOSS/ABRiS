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
import za.co.absa.abris.avro.read.confluent.SchemaManager
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
    new Column(AvroDataToCatalyst(data.expr, Some(jsonFormatSchema), None, removeSchemaId = false))
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
  def from_avro(data: Column, schemaRegistryConf: Map[String,String]): Column = {
    new Column(sql.AvroDataToCatalyst(data.expr, None, Some(schemaRegistryConf), removeSchemaId = false))
  }

  /**
   * Converts a binary column of confluent avro format into its corresponding catalyst value using schema registry.
   * The schema id is removed from start of the avro payload, but it's not used. You still need to provide some schema
   * id in the schemaRegistryConf.
   * The schema loaded from schema registry must match the read data, otherwise the behavior is undefined:
   * it may fail or return arbitrary result.
   *
   * @param data the binary column.
   * @param schemaRegistryConf schema registry configuration.
   *
   */
  def from_confluent_avro(data: Column, schemaRegistryConf: Map[String,String]): Column = {
    new Column(sql.AvroDataToCatalyst(data.expr, None, Some(schemaRegistryConf), removeSchemaId = true))
  }

  /**
   * Converts a binary column of confluent avro format into its corresponding catalyst value using provided schema.
   * The schema id is removed from start of the avro payload, but it's not used.
   * The specified schema must match the read data, otherwise the behavior is undefined:
   * it may fail or return arbitrary result.
   *
   * @param data the binary column.
   * @param jsonFormatSchema the avro schema in JSON string format.
   *
   */
  def from_confluent_avro(data: Column, jsonFormatSchema: String): Column = {
    new Column(sql.AvroDataToCatalyst(data.expr, Some(jsonFormatSchema), None, removeSchemaId = true))
  }

  /**
   * Converts a column into binary of avro format.
   * Schema is generated automatically.
   *
   */
  def to_avro(data: Column): Column = {
    new Column(CatalystDataToAvro(data.expr, SchemaProvider(), None, prependSchemaId = false))
  }

  /**
   * Converts a column into binary of avro format.
   *
   * @param data column to be converted to avro
   * @param jsonFormatSchema schema used for conversion
   */
  def to_avro(data: Column, jsonFormatSchema: String): Column = {
    new Column(sql.CatalystDataToAvro(data.expr, SchemaProvider(jsonFormatSchema), None, prependSchemaId = false))
  }

  /**
   * Converts a column into binary of avro format and store the used schema in schema registry.
   * Schema is generated automatically.
   *
   * @param data column to be converted to avro
   * @param schemaRegistryConf schema registry configuration
   */
  def to_avro(data: Column, schemaRegistryConf: Map[String,String]): Column = {
    val name = schemaRegistryConf(SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY)
    val namespace = schemaRegistryConf(SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY)

    new Column(sql.CatalystDataToAvro(
      data.expr, SchemaProvider(name, namespace), Some(schemaRegistryConf), prependSchemaId = false))
  }

  /**
   * Converts a column into binary of avro format and store the used schema in schema registry
   *
   * @param data column to be converted to avro
   * @param jsonFormatSchema schema used for conversion
   * @param schemaRegistryConf schema registry configuration
   */
  def to_avro(data: Column, jsonFormatSchema: String, schemaRegistryConf: Map[String,String]): Column = {
    new Column(sql.CatalystDataToAvro(
      data.expr, SchemaProvider(jsonFormatSchema), Some(schemaRegistryConf), prependSchemaId = false))
  }

  /**
   * Converts a column into binary of avro format, store the used schema in schema registry and prepend the schema id
   * to avro payload (according to confluent avro format)
   * Schema is generated automatically.
   *
   * @param data column to be converted to avro
   * @param schemaRegistryConf schema registry configuration
   */
  def to_confluent_avro(data: Column, schemaRegistryConf: Map[String,String]): Column = {

    val name = schemaRegistryConf(SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY)
    val namespace = schemaRegistryConf(SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY)

    new Column(sql.CatalystDataToAvro(
      data.expr, SchemaProvider(name, namespace), Some(schemaRegistryConf), prependSchemaId = true))
  }

  /**
   * Converts a column into binary of avro format, store the used schema in schema registry and prepend the schema id
   * to avro payload (according to confluent avro format)
   *
   * @param data column to be converted to avro
   * @param schemaRegistryConf schema registry configuration
   */
  def to_confluent_avro(data: Column, jsonFormatSchema: String, schemaRegistryConf: Map[String,String]): Column = {

    new Column(sql.CatalystDataToAvro(
      data.expr, SchemaProvider(jsonFormatSchema), Some(schemaRegistryConf), prependSchemaId = true))
  }

  // scalastyle:on: method.name
}
