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

package za.co.absa.abris.avro.schemas.impl

import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.schemas.SchemasProcessor

/**
 * This class is capable of producing Avro and Spark schemas from a plain Avro schema.
 */
class AvroToSparkProcessor(plainAvroSchema: String) extends SchemasProcessor {  
  
  def getAvroSchema(): Schema = {
    AvroSchemaUtils.parse(plainAvroSchema)
  }
  
  def getSparkSchema(): StructType = {
    SparkAvroConversions.toSqlType(getAvroSchema())
  }
}