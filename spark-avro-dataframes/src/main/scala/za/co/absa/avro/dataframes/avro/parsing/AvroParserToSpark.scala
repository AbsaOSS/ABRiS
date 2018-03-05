package za.co.absa.avro.dataframes.avro.parsing

import scala.collection.JavaConverters.asScalaBufferConverter
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import com.databricks.spark.avro.SchemaConverters

/**
 * This class provides methods to convert Avro's GenericRecords to Spark's GenericRowWithSchemas.
 */
class AvroParser extends Serializable {
  
  /**
   * Converts Avro's GenericRecords to Spark's GenericRowWithSchemas.
   * 
   * This method relies on the Avro schema being set into the incoming record.
   */
  def parse(avroRecord: GenericRecord): GenericRowWithSchema = {    
    val structType = getSqlTypeForSchema(avroRecord.getSchema)    
    val avroDataArray: Array[Any] = new Array(avroRecord.getSchema.getFields.size())
    for (field <- avroRecord.getSchema.getFields.asScala) {      
      avroDataArray(field.pos()) = avroRecord.get(field.pos())
    }    
    new GenericRowWithSchema(avroDataArray, structType)
  }
  
  /**
   * Translates an Avro Schema into a Spark's StructType.
   * 
   * Relies on Databricks Spark-Avro library to do the job.
   */
  def getSqlTypeForSchema(schema: Schema): StructType = {
    SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
  }
}