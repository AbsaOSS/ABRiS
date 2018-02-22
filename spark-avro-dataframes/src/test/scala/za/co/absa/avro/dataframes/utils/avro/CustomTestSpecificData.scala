package za.co.absa.avro.dataframes.utils.avro

import org.apache.avro.generic.IndexedRecord
import org.apache.spark.sql.catalyst.expressions.GenericRow
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import za.co.absa.avro.dataframes.avro.ScalaSpecificData

object CustomTestSpecificData {
  private val INSTANCE = new CustomTestSpecificData()
  def get(): CustomTestSpecificData = INSTANCE
}

class CustomTestSpecificData extends za.co.absa.avro.dataframes.avro.ScalaSpecificData {
  override def getField(record: Object, name: String, position: Int): Object = {
    try {
      record.asInstanceOf[IndexedRecord].get(position)
    }
    catch {
      case _: Throwable => record.asInstanceOf[GenericRow].get(position).asInstanceOf[Object] 
    }
  }    
}