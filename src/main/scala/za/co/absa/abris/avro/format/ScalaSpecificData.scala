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

package za.co.absa.abris.avro.format

import java.util.Collection

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificData


object ScalaSpecificData {
  private val INSTANCE = new ScalaSpecificData()
  def get(): ScalaSpecificData = INSTANCE
}

/**
 * This class forces Avro to use a specific Record implementation (ScalaRecord), which converts
 * nested records to Spark Rows at read time.
 */
class ScalaSpecificData extends SpecificData {  
  
  override def newRecord(old: Object, schema: Schema): Object = {
    new ScalaAvroRecord(schema)
  }    
  
  /**
   * Provides compatibility between Java and Scala collections, since Avro writers are written in Java
   * and use a custom implementation of Collection, but this library also uses those writers to convert
   * records into Array[Byte].
   */
  override def isArray(datum: Object): Boolean = {    
    if (datum.isInstanceOf[Collection[Any]]) true
    else if (datum.isInstanceOf[Iterable[Any]] && !datum.isInstanceOf[Map[Any,Any]]) true // does not make sense for a map to be an array, check unnecessary for Java since Map is not a Collection
    else datum.isInstanceOf[Array[Object]]    
  }
}