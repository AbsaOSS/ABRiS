package za.co.absa.avro.dataframes.parsing

import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificData
import org.apache.avro.Schema
import org.apache.avro.io.Decoder;
import scala.collection._

/**
 * Avro uses its own representations of Strings and Arrays, as well as uses a Java HashMap to back map records.
 * 
 * As those types are not directly translatable to Scala, this class overrides SpecificDatumReader to manually perform the translations at runtime. 
 */
class ScalaDatumReader[T](schema: Schema) extends SpecificDatumReader[T](schema, schema, ScalaSpecificData.get()) with Serializable {
  
  override def readString(old: Object, expected: Schema, in: Decoder) = {
    in.readString()
  }
  
	override def newArray(old: Object, size: Int, schema: Schema) = {				
		new mutable.ListBuffer[Any]()		
	}	

	override def addToArray(array: Object, post: Long, e: Object) {
	  array.asInstanceOf[mutable.ListBuffer[Any]].append(e)	
	} 
	
  override def newMap(old: Object, size: Int): Object = {
    new mutable.HashMap[Any,Any]
  }	
  
  override def addToMap(map: Object, key: Object, value: Object) {
    map.asInstanceOf[mutable.HashMap[Any,Any]].put(key, value)
  } 
}