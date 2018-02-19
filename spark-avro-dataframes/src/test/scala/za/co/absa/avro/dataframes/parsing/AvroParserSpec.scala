package za.co.absa.avro.dataframes.parsing

import java.lang.Byte
import java.lang.Double
import java.lang.Float
import java.lang.Long
import java.lang.Short
import java.math.BigInteger
import java.sql.Timestamp
import java.util.Arrays
import java.util.Date

import scala.collection._
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.convert.Wrappers.JListWrapper
import scala.collection.immutable.Map

import org.scalatest._
import org.scalatest.FlatSpec

import za.co.absa.avro.dataframes.utils.AvroParsingTestUtils
import java.util.ArrayList
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.AvroRuntimeException
import org.scalatest.Matchers._
import java.util.HashMap
import org.apache.spark.sql.catalyst.expressions.GenericRow

class AvroParserSpec extends FlatSpec {
  
  private val avroParser = new AvroParser()
  
  behavior of "AvroParser"  
  
  it should "convert Avro's GenericRecord to Row using informed PLAIN Schema" in {        
    val map = new HashMap[String,Long]()
    map.put("entry1", 1l)
    map.put("entry2", 2l)
    
    val dataMap = Map[String,Any] (        
        "string"     -> "aString",
        "int"        -> Integer.MAX_VALUE,
        "long"       -> Long.MAX_VALUE,
        "double"     -> Double.MAX_VALUE,
        "float"      -> Float.MAX_VALUE,
        "short"      -> Short.MAX_VALUE,
        "byte"       -> Byte.MAX_VALUE,
        "anyOption"  -> "anOption",
        "seqAny"     -> new ArrayList(Arrays.asList("elem1", "elem2")),            
        "mapAny"     -> map,
        "setAny"     -> new java.util.HashSet[String](Arrays.asList("elem1", "elem2")),
        "date"       -> new Date(System.currentTimeMillis()).getTime,
        "timestamp"  -> new Timestamp(System.currentTimeMillis()).getTime,
        "bigDecimal" -> BigDecimal.apply(Double.MAX_VALUE).toString(),
        "bigInteger" -> BigInteger.valueOf(Long.MAX_VALUE).toString()        
    )    
    
    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(dataMap, AvroParsingTestUtils.PLAIN_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)    
    
    for (entry <- dataMap) {            
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }
  }  
  
  it should "convert Avro's GenericRecord to Row using informed NESTED Schema" in {    
    case class Address(streetaddress: String, city: String, previousAddresses: Array[String])    
    val addressSchema = """{"type":  "record",
                                    "name":   "address",
                                    "fields": [
                                        {"name": "streetaddress",     "type": "string"},
                                        {"name": "city",              "type": "string"},
                                        {"name": "previousAddresses", "type": {"type": "array", "items": "string"}}                                    
                                              ]                                  
                           }"""
    
    val addressData = Map[String,Object](
      "streetaddress"     -> "street F",
      "city"              -> "Prague",
      "previousAddresses" -> new ArrayList(Arrays.asList("street A", "street B"))
    )
    val addressRecord = AvroParsingTestUtils.mapToGenericRecord(addressData, addressSchema)
    
    val dataMap = Map[String,Object] (        
        "firstname" ->     "Felipe",
        "lastname" ->      "Melo",
        "addressRecord" -> addressRecord   
    )
    
    val avroRecord = AvroParsingTestUtils.mapToCustomRecord(dataMap, AvroParsingTestUtils.COMPLEX_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)      
    
    for (entry <- dataMap) {        
      assertEquals(entry._2, resultRow.getAs(entry._1))
    }    
  } 
  
  private def assertEquals(original: Any, retrieved: Any) = {        
    original.getClass().getName match {
      case "java.util.ArrayList" => retrieved.asInstanceOf[mutable.ListBuffer[Any]].toList == original.asInstanceOf[java.util.ArrayList[Any]].toList        
      case "java.util.HashSet" =>   retrieved.asInstanceOf[mutable.ListBuffer[Any]].toList == original.asInstanceOf[java.util.Set[Any]].toList
      case "java.util.HashMap" =>   retrieved.asInstanceOf[mutable.HashMap[Any,Any]] == original.asInstanceOf[java.util.HashMap[Any,Any]].toMap
      case "za.co.absa.avro.dataframes.parsing.CustomRecord" => {        
        val originalRecord = original.asInstanceOf[ScalaRecord]
        val retrievedRecord = retrieved.asInstanceOf[GenericRow]
        for (i <- 0 until originalRecord.getValues().length) {
          if (originalRecord.get(i) != retrievedRecord.get(i)) {
            false
          }          
        }
        true
      }
      case _ => original == retrieved
    }
    
  }
}