package com.databricks.spark.avro

import org.apache.spark.sql.types.DataType
import com.databricks.spark.avro.SchemaConverters.SchemaType
import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType
import org.apache.avro.SchemaBuilder.RecordBuilder
import org.apache.avro.SchemaBuilder

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