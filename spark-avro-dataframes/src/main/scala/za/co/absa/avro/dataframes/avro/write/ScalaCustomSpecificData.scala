package za.co.absa.avro.dataframes.avro.write

import org.apache.avro.generic.IndexedRecord
import org.apache.spark.sql.catalyst.expressions.GenericRow
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object ScalaCustomSpecificData {
  private val INSTANCE = new ScalaCustomSpecificData()
  def get(): ScalaCustomSpecificData = INSTANCE
}

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