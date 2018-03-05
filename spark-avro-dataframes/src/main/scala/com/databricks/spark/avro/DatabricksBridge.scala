package com.databricks.spark.avro

import org.apache.spark.sql.types.DataType
import com.databricks.spark.avro.SchemaConverters.SchemaType
import org.apache.avro.Schema

object DatabricksBridge {  
  
    def getNewRecordNamespace(
      elementDataType: DataType,
      currentRecordNamespace: String,
      elementName: String): String = SchemaConverters.getNewRecordNamespace(elementDataType, currentRecordNamespace, elementName)
      
    def toSqlType(avroSchema: Schema): SchemaType = SchemaConverters.toSqlType(avroSchema)      
}