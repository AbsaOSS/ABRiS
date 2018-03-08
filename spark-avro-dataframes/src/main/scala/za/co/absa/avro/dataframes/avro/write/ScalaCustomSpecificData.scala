package za.co.absa.avro.dataframes.avro.write

import org.apache.avro.generic.IndexedRecord
import org.apache.spark.sql.catalyst.expressions.GenericRow
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object ScalaCustomSpecificData {
  private val INSTANCE = new ScalaCustomSpecificData()
  def get(): ScalaCustomSpecificData = INSTANCE
}

/**
 * This class redefines the way fields are retrieved from a record since they could be either
 * an IndexedRecord (if written by Avro library) or a GenericRow, if written this library.
 */
class ScalaCustomSpecificData extends za.co.absa.avro.dataframes.avro.format.ScalaSpecificData {
    
  override def getField(record: Object, name: String, position: Int): Object = {
    try {
      record.asInstanceOf[IndexedRecord].get(position)
    }
    catch {
      case _: Throwable => record.asInstanceOf[GenericRow].get(position).asInstanceOf[Object] 
    }
  }    
}