package za.co.absa.abris.avro.schemas

import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType

/**
 * Trait for objects that can produce Avro and Spark schemas from each other.
 */
trait SchemasProcessor extends Serializable {  
  def getAvroSchema(): Schema
  def getSparkSchema(): StructType
}