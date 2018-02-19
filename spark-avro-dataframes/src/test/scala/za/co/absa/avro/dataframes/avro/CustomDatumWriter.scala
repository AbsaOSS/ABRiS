package za.co.absa.avro.dataframes.avro

import java.util.Collection

import scala.collection.Iterable
import scala.collection.JavaConversions.asJavaIterator

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificDatumWriter


/* Used for tests only.
 * Provides compatibility between the custom reader and the test data.
 */
class CustomDatumWriter[T](schema: Schema) extends SpecificDatumWriter[T](schema, CustomTestSpecificData.get()) {
  
  /**
   * Tries to find the array size. Tries to cast the incoming Object to a Scala collection first, 
   * then to a Java collection, throwing if neither is a match. 
   */
  override def getArraySize(array: Object) = {
    try {
      array.asInstanceOf[Iterable[Any]].size
    }
    catch {
      case _: Throwable => array.asInstanceOf[Collection[Any]].size                        
    }
  }	  
  
  override def getArrayElements(array: Object): java.util.Iterator[_ <: Object] = {    
    try {
      array.asInstanceOf[Iterable[_ <: Object]].iterator
    }
    catch {
      case _: Throwable => array.asInstanceOf[Collection[_ <: Object]].iterator                        
    }    
  }  
}