package za.co.absa.avro.dataframes.parsing

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificData
import org.apache.avro.generic.IndexedRecord

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
    new ScalaRecord(schema)
  }    
}