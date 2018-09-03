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

package za.co.absa.abris.avro.read

import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.Schema
import org.apache.avro.io.Decoder
import scala.collection._
import za.co.absa.abris.avro.format.ScalaSpecificData

/**
 * Avro uses its own representations of Strings and Arrays, as well as a Java HashMap to back map records.
 * 
 * As those types are not directly translatable to Scala, this class overrides SpecificDatumReader to manually perform the translations at runtime. 
 */
class ScalaDatumReader[T](writerSchema: Schema, readerSchema: Schema) extends SpecificDatumReader[T](writerSchema, readerSchema, ScalaSpecificData.get()) with Serializable {

  def this(schema: Schema) {
    this(schema, schema)
  }

  /**
   * This method was overriden to force all Strings to be read as Scala strings
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
	 * This method was overriden since Avro's original implementation relies on Java HashMaps, which
	 * are not directly translatable to Spark MapType.
	 */
  override def newMap(old: Object, size: Int): Object = {
    new mutable.HashMap[Any,Any]
  }	
  
  override def addToMap(map: Object, key: Object, value: Object) {
    map.asInstanceOf[mutable.HashMap[Any,Any]].put(key, value)
  } 
}