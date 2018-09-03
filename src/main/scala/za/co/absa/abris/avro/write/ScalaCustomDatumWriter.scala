/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.abris.avro.write

import java.nio.ByteBuffer
import java.util.Collection

import org.apache.avro.Schema
import org.apache.avro.generic.GenericFixed
import org.apache.avro.io.Encoder
import org.apache.avro.specific.SpecificDatumWriter

import scala.collection.Iterable
import scala.collection.JavaConversions.asJavaIterator

/**
 * This class redefines Avro data writing methods that cope with collections and arrays in order to provide 
 * compatibility between Java and Scala while using Avro's original library.
 */
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
  
  override def writeFixed(schema: Schema, datum: Object, out: Encoder) = {    
    try {
      out.writeFixed(datum.asInstanceOf[GenericFixed].bytes(), 0, schema.getFixedSize())
    }
    catch {
      case _: Throwable => out.writeFixed(datum.asInstanceOf[ByteBuffer].array(), 0, schema.getFixedSize());      
    }
  }  
}