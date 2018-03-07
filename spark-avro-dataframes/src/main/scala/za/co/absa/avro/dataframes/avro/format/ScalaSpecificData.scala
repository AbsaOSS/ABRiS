package za.co.absa.avro.dataframes.avro.format

import java.util.Collection

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificData


object ScalaSpecificData {
  private val INSTANCE = new ScalaSpecificData()
  def get(): ScalaSpecificData = INSTANCE
}

/**
 * This class forces Avro to use a specific Record implementation (ScalaRecord), which converts
 * nested records to Spark Rows at read time.
 */
class ScalaSpecificData extends SpecificData {  
  
  override def newRecord(old: Object, schema: Schema): Object = {
    new ScalaAvroRecord(schema)
  }    
  
  override def isArray(datum: Object): Boolean = {    
    if (datum.isInstanceOf[Collection[Any]]) true
    if (datum.isInstanceOf[Iterable[Any]]) true
    couldBeArrayType(datum)
  }
  
  private def couldBeArrayType(datum: Object): Boolean = {
    try {
      datum.asInstanceOf[Array[Object]]
      true
    }
    catch {
      case _: Throwable => false
    }
  }
}