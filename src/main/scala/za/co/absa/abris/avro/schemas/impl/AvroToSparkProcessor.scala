package za.co.absa.abris.avro.schemas.impl

import za.co.absa.abris.avro.schemas.SchemasProcessor
import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.format.SparkAvroConversions

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