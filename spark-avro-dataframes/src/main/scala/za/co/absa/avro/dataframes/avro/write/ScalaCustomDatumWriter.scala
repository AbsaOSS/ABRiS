package za.co.absa.avro.dataframes.avro.write

import java.util.Collection
import scala.collection.Iterable
import scala.collection.JavaConversions.asJavaIterator
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificDatumWriter
import java.util.Arrays

class ScalaCustomDatumWriter[T] extends SpecificDatumWriter[T](ScalaCustomSpecificData.get()) {
  
  /**
   * Tries to find the array size. Tries to cast the incoming Object to a Scala collection first, 
   * then to a Java collection, throwing if neither is a match. 
   * 
   * This is necessary since Avro converters on the Scala side expect hard-coded Seqs (i.e. the try to explicitly
   * cast arrays to Seq[Any]), whereas on the Java side a Collection is expected (i.e. explicitly attempt to cast
   * arrays to Collection<Object>).
   */
  override def getArraySize(array: Object) = {
    try {
      array.asInstanceOf[Iterable[Any]].size
    }
    catch {
      case _: Throwable => try {
        array.asInstanceOf[Collection[Any]].size                        
      }
      catch {
        case _: Throwable => array.asInstanceOf[Array[Object]].size
      }
    }
  }	  
  
  override def getArrayElements(array: Object): java.util.Iterator[_ <: Object] = {    
    try {
      array.asInstanceOf[Iterable[_ <: Object]].iterator
    }
    catch {
      case _: Throwable => try {
        array.asInstanceOf[Collection[_ <: Object]].iterator                        
      }
      catch {
        case _: Throwable => array.asInstanceOf[Array[Object]].iterator
      }
    }    
  }  
}