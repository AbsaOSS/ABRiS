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
import za.co.absa.abris.avro.sql.{AvroDataToCatalyst, CatalystDataToAvro}
import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig, ToAvroConfig}


// scalastyle:off: object.name
object functions {
// scalastyle:on: object.name
// scalastyle:off: method.name

  /**
   *
   * @param column containing data for conversion
   * @param config Abris configuration
   * @return column containing data in avro format
   */
  def to_avro(column: Column, config: ToAvroConfig): Column = {
    new Column(CatalystDataToAvro(
      column.expr,
      config.schemaString,
      config.schemaId
    ))
  }

  /**
   *
   * @param column containing data for conversion
   * @param schema avro schema
   * @return column containing data in avro format
   */
  def to_avro(column: Column, schema: String): Column = {
    val config = AbrisConfig
      .toSimpleAvro
      .provideSchema(schema)

    to_avro(column, config)
  }

  /**
   *
   * @param column column containing data for conversion
   * @param config Abris configuration
   * @return column with converted data
   */
  def from_avro(column: Column, config: FromAvroConfig): Column = {
    new Column(AvroDataToCatalyst(
      column.expr,
      config.schemaString,
      config.schemaRegistryConf,
      config.schemaRegistryConf.isDefined
    ))
  }

  /**
   *
   * @param column column containing data for conversion
   * @param schema avro schema
   * @return column with converted data
   */
  def from_avro(column: Column, schema: String): Column = {
    val config = AbrisConfig
      .fromSimpleAvro
      .provideSchema(schema)

    from_avro(column, config)
  }

  // scalastyle:on: method.name
}
