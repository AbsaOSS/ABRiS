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

import org.apache.avro.Schema
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.StructType
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils

/**
  * This object works as a factory for encoders from Avro schemas into Spark row encoders.
  */
private[avro] object AvroToRowEncoderFactory {

  def createRowEncoder(schema: StructType): ExpressionEncoder[Row] = {
    RowEncoder(schema)
  }

  def createRowEncoder(schema: Schema): ExpressionEncoder[Row] = {
    createRowEncoder(SparkAvroConversions.toSqlType(schema))
  }

  def createRowEncoder(schemaPath: String): ExpressionEncoder[Row] = {
    createRowEncoder(AvroSchemaUtils.load(schemaPath))
  }

  def createRowEncoder(schemaRegistryConf: Map[String,String]): ExpressionEncoder[Row] = {
    createRowEncoder(AvroSchemaUtils.loadForValue(schemaRegistryConf))
  }
}
