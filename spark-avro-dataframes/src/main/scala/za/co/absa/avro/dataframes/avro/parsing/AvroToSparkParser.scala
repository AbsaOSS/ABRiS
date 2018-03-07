package za.co.absa.avro.dataframes.avro.parsing

import scala.collection.JavaConverters.asScalaBufferConverter
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import com.databricks.spark.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.GenericRow
import com.databricks.spark.avro.DatabricksAdapter
import scala.collection.mutable.HashMap
import za.co.absa.avro.dataframes.avro.format.SparkAvroConversions

/**
 * This class provides methods to convert Avro's GenericRecords to Spark's GenericRowWithSchemas.
 */
class AvroToSparkParser extends Serializable {
  
  private var schemaToSql = new HashMap[String,StructType]
  
  /**
   * Converts Avro's GenericRecords to Spark's GenericRowWithSchemas.
   * 
   * This method relies on the Avro schema being set into the incoming record.
   * 
   * This method caches StructTypes for schema names, thus, it is SENSITIVE to schema naming.
   * If a schema is changed, make sure to either, call 'reset' on the current instance or create a new one.
   */
  def parse(avroRecord: GenericRecord): GenericRowWithSchema = {    
    val structType = getSqlType(avroRecord.getSchema)    
    val avroDataArray: Array[Any] = new Array(avroRecord.getSchema.getFields.size())
    for (field <- avroRecord.getSchema.getFields.asScala) {      
      avroDataArray(field.pos()) = avroRecord.get(field.pos())
    }    
    new GenericRowWithSchema(avroDataArray, structType)
  }
  
  private def getSqlType(schema: Schema): StructType = {
    schemaToSql.getOrElseUpdate(schema.getName, SparkAvroConversions.toSqlType(schema))    
  }
  
  /**
   * Cleans up the cache of StructTypes. 
   * 
   * There should be no reason to invoke this method unless a schema already processed by this instance
   * has changed.
   */
  def reset = schemaToSql.clear()
}