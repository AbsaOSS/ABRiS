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

package za.co.absa.abris.avro.format

import org.apache.avro.Schema
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types._
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils


/**
 * This class provides conversions between Avro and Spark schemas and data.
 */
object SparkAvroConversions {

  /**
   * Converts a Spark's SQL type into an Avro schema, using specific names and namespaces for the schema.
   */
  def toAvroSchema(
      structType: StructType,
      schemaName: String,
      schemaNamespace: String): Schema = {
    SchemaConverters.toAvroType(structType, false, schemaName, schemaNamespace)
  }

  /**
   * Translates an Avro Schema into a Spark's StructType.
   *
   * Relies on Spark-Avro library to do the job.
   */
  def toSqlType(schema: String): StructType = toSqlType(AvroSchemaUtils.parse(schema))

  /**
   * Translates an Avro Schema into a Spark's StructType.
   *
   * Relies on Spark-Avro library to do the job.
   */
  def toSqlType(schema: Schema): StructType = {
    SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
  }

}
