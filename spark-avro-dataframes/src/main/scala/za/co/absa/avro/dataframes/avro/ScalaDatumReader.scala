package za.co.absa.avro.dataframes.avro

import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.specific.SpecificData
import org.apache.avro.Schema
import org.apache.avro.io.Decoder;
import scala.collection._

/**
 * Avro uses its own representations of Strings and Arrays, as well as a Java HashMap to back map records.
 * 
 * As those types are not directly translatable to Scala, this class overrides SpecificDatumReader to manually perform the translations at runtime. 
 */
class ScalaDatumReader[T](schema: Schema) extends SpecificDatumReader[T](schema, schema, ScalaSpecificData.get()) with Serializable {
  
  /**
   * This method was overriden to force all Strings to be forced as Scala strings
   * instead of Avro's org.apache.avro.util.Utf8.
   */
  override def readString(old: Object, expected: Schema, in: Decoder) = {
    in.readString()
  }
  
  /**
   * This method was overriden so that every collection is read as a Scala's
   * mutable.ListBuffer instead of Avro's GenericData.ARRAY. 
   */
	override def newArray(old: Object, size: Int, schema: Schema) = {				
		new mutable.ListBuffer[Any]()
	}	

	override def addToArray(array: Object, post: Long, e: Object) {
	  array.asInstanceOf[mutable.ListBuffer[Any]].append(e)	
	} 
	
	/**
	 * This method was overriden simple Avro's original implementation relies on Java HashMaps, which
	 * are not directly translatable to Spark MapType.
	 */
  override def newMap(old: Object, size: Int): Object = {
    new mutable.HashMap[Any,Any]
  }	
  
  override def addToMap(map: Object, key: Object, value: Object) {
    map.asInstanceOf[mutable.HashMap[Any,Any]].put(key, value)
  } 
}