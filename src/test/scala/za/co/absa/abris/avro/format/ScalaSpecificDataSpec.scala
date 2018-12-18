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

import java.util.Arrays

import org.scalatest.FlatSpec
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.examples.data.generation.TestSchemas

class ScalaSpecificDataSpec extends FlatSpec {
    
  private val specificData = new ScalaSpecificData
  
  behavior of "ScalaSpecificData"
  
  it should "identify Java Collections as arrays" in {
    val list = new java.util.ArrayList(Arrays.asList(1,2,3))
    val set = new java.util.HashSet(Arrays.asList(1,2,3))
    val deque = new java.util.ArrayDeque(Arrays.asList(1,2,3))
    
    val map = new java.util.HashMap[Int,Int]()
    map.put(1, 2)
    map.put(2, 3)
    
    assert(specificData.isArray(list))
    assert(specificData.isArray(set))
    assert(specificData.isArray(deque))
    assert(!specificData.isArray(map))
  }
  
  it should "identify Scala Iterables as arrays" in {
    val list = List(1,2,3)
    val seq = Seq(1,2,3)
    val set = Set(1,2,3)
    val map = Map(1 -> 2, 2 -> 3)
    
    assert(specificData.isArray(list))
    assert(specificData.isArray(set))    
    assert(!specificData.isArray(map))    
  }
  
  it should "identify Scala Arrays[Object] as arrays" in {
    val arrayObj = Array(new Integer(1), new Integer(2), new Integer(3))
    val arrayAny = Array(1, 2, 3)
    
    assert(specificData.isArray(arrayObj))
    assert(!specificData.isArray(arrayAny))
  }
  
  it should "create ScalaAvroRecords when asked for new records" in {
    val schema = AvroSchemaUtils.parse(TestSchemas.COMPLEX_SCHEMA_SPEC)
    val record = specificData.newRecord(null, schema)
    assert(record.isInstanceOf[ScalaAvroRecord])
  }
}