package za.co.absa.avro.dataframes.avro

import org.apache.avro.generic.IndexedRecord
import za.co.absa.avro.dataframes.parsing.ScalaSpecificData
import org.apache.spark.sql.catalyst.expressions.GenericRow
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object CustomTestSpecificData {
  private val INSTANCE = new CustomTestSpecificData()
  def get(): CustomTestSpecificData = INSTANCE
}

class CustomTestSpecificData extends ScalaSpecificData {
  override def getField(record: Object, name: String, position: Int): Object = {
    try {
      record.asInstanceOf[IndexedRecord].get(position)
    }
    catch {
      case _: Throwable => record.asInstanceOf[GenericRow].get(position).asInstanceOf[Object] 
    }
  }    
}