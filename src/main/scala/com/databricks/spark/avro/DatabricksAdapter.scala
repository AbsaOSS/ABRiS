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

package com.databricks.spark.avro

import com.databricks.spark.avro.SchemaConverters.SchemaType
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder.RecordBuilder
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * This class works as a bridge between Databricks' and the current library.
 * 
 * It was created in order to prevent changes to Databricks from bugging this library, since the changes 
 * can be dealt with at a single location. 
 */
object DatabricksAdapter {

  def getNewRecordNamespace(
    elementDataType:        DataType,
    currentRecordNamespace: String,
    elementName:            String): String = SchemaConverters.getNewRecordNamespace(elementDataType, currentRecordNamespace, elementName)

  def toSqlType(avroSchema: Schema): SchemaType = SchemaConverters.toSqlType(avroSchema)

  def createConverterToSQL(
    sourceAvroSchema: Schema,
    targetSqlType:    DataType): AnyRef => AnyRef = {
    SchemaConverters.createConverterToSQL(sourceAvroSchema, targetSqlType)
  }
        
  def convertStructToAvro[T](
      structType: StructType,
      schemaBuilder: RecordBuilder[T],
      recordNamespace: String): T = {
    SchemaConverters.convertStructToAvro(structType, schemaBuilder, recordNamespace)
  }
}