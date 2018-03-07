package za.co.absa.avro.dataframes.avro.format

import java.lang.Double
import java.lang.Float
import java.lang.Long
import java.lang.Boolean
import java.util.ArrayList
import java.util.HashMap

import scala.collection._
import scala.collection.JavaConverters._

import org.scalatest.FlatSpec
import za.co.absa.avro.dataframes.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.avro.dataframes.utils.TestSchemas
import java.nio.ByteBuffer
import java.util.Arrays
import za.co.absa.avro.dataframes.utils.avro.fixed.FixedString

import org.apache.spark.sql.Row

class ScalaAvroRecordSpec extends FlatSpec {

  behavior of "ScalaAvroRecord"

  it should "identify equal records if fields inserted at specified positions" in {    
    val data = new mutable.HashMap[Int,Object]()
    data.put(0, ByteBuffer.wrap("ASimpleString".getBytes))
    data.put(1, "a string")
    data.put(2, new Integer(Integer.MAX_VALUE))
    data.put(3, new Long(Long.MAX_VALUE))
    data.put(4, new Double(Double.MAX_VALUE))
    data.put(5, new Float(Float.MAX_VALUE))
    data.put(6, Boolean.TRUE)
    data.put(7, new ArrayList(Arrays.asList("elem1", "elem2")))
    data.put(8, new FixedString("ASimpleString"))
    
    val map = new HashMap[String, java.util.ArrayList[Long]]()
    map.put("entry1", new ArrayList(java.util.Arrays.asList(new Long(1), new Long(2))))
    map.put("entry2", new ArrayList(java.util.Arrays.asList(new Long(3), new Long(4))))    
    data.put(9, map)
    
    val schema = AvroSchemaUtils.parse(TestSchemas.NATIVE_COMPLETE_SCHEMA)
    val record = new ScalaAvroRecord(schema)
        
    data.foreach(entry => record.put(entry._1, entry._2))   
    for (key <- data.keySet if key != 0) assert(record.get(key) == data(key)) 
    assert(record.get(0) == data(0).asInstanceOf[ByteBuffer].array())
  }

  it should "identify equal records if fields inserted with specified names" in {
    val data = new mutable.HashMap[String,Object]()
    data.put("bytes", ByteBuffer.wrap("ASimpleString".getBytes))
    data.put("string", "a string")
    data.put("int", new Integer(Integer.MAX_VALUE))
    data.put("long", new Long(Long.MAX_VALUE))
    data.put("double", new Double(Double.MAX_VALUE))
    data.put("float", new Float(Float.MAX_VALUE))
    data.put("boolean", Boolean.TRUE)
    data.put("array", new ArrayList(Arrays.asList("elem1", "elem2")))
    data.put("fixed", new FixedString("ASimpleString"))
    
    val map = new HashMap[String, java.util.ArrayList[Long]]()
    map.put("entry1", new ArrayList(java.util.Arrays.asList(new Long(1), new Long(2))))
    map.put("entry2", new ArrayList(java.util.Arrays.asList(new Long(3), new Long(4))))    
    data.put("map", map)
    
    val schema = AvroSchemaUtils.parse(TestSchemas.NATIVE_COMPLETE_SCHEMA)
    val record = new ScalaAvroRecord(schema)
        
    data.foreach(entry => record.put(entry._1, entry._2))   
    for (key <- data.keySet if key != "bytes") assert(record.get(key) == data(key)) 
    assert(record.get("bytes") == data("bytes").asInstanceOf[ByteBuffer].array())    
  }
  
  it should "convert nested records into Spark Rows" in {    
    val nestedSchema = AvroSchemaUtils.parse(TestSchemas.NATIVE_SIMPLE_NESTED_SCHEMA)
    val nested = new ScalaAvroRecord(nestedSchema)
    nested.put("int", new Integer(Integer.MAX_VALUE))
    nested.put("long", new Long(Long.MAX_VALUE))
    
    val outerSchema = AvroSchemaUtils.parse(TestSchemas.NATIVE_SIMPLE_OUTER_SCHEMA)
    val outer = new ScalaAvroRecord(outerSchema)
    outer.put("name", "whatever")
    outer.put("nested", nested)    
    
    assert(outer.get("nested").isInstanceOf[Row])
  }
}