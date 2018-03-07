package za.co.absa.avro.dataframes.avro.format

import org.scalatest.FlatSpec
import java.util.Arrays
import za.co.absa.avro.dataframes.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.avro.dataframes.utils.TestSchemas

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