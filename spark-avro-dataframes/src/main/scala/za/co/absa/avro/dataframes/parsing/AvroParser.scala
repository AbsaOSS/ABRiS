package za.co.absa.avro.dataframes.parsing        

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

import com.databricks.spark.avro.SchemaConverters
import org.apache.spark.sql.Row
import scala.math.BigInt

/**
 * This class provides methods to convert Avro's GenericRecords to Spark's GenericRowWithSchemas.
 */
class AvroParser extends Serializable {
  
  implicit class Utf8Unwrapper(value: Utf8) {
    def unwrap() = {
      value.toString()
    }
  }
  
  /**
   * Converts Avro's GenericRecords to Spark's GenericRowWithSchemas.
   * 
   * This method relies on the Avro schema being set into the incoming record.
   */
  def parse(avroRecord: GenericRecord): GenericRowWithSchema = {    
    val structType: StructType = getSqlTypeForSchema(avroRecord.getSchema)    
    val avroDataArray: Array[Any] = new Array(avroRecord.getSchema.getFields.size())
    
    for (field <- avroRecord.getSchema.getFields.asScala) {      
      avroDataArray(field.pos()) = avroRecord.get(field.pos())
    }    
    new GenericRowWithSchema(analyzeAvroData(avroDataArray), structType)
  }
  
  private def analyzeAvroData(avroDataArray: Array[Any]): Array[Any] = {    
    val analyzedArray: Array[Any] = new Array[Any](avroDataArray.size)
    for (i <- 0 until avroDataArray.size) {      
      analyzedArray(i) = avroDataArray(i)
    }
    analyzedArray
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