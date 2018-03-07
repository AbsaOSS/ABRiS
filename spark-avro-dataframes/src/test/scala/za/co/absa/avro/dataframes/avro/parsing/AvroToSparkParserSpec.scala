package za.co.absa.avro.dataframes.parsing

import java.lang.Double
import java.lang.Float
import java.lang.Long
import java.lang.Boolean
import java.util.ArrayList
import java.util.HashMap

import scala.collection._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.Map

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest._
import org.scalatest.FlatSpec

import za.co.absa.avro.dataframes.utils.AvroParsingTestUtils
import za.co.absa.avro.dataframes.utils.TestSchemas
import java.util.Arrays
import java.nio.ByteBuffer
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericFixed
import za.co.absa.avro.dataframes.utils.avro.fixed.FixedString
import za.co.absa.avro.dataframes.avro.format.ScalaAvroRecord
import za.co.absa.avro.dataframes.avro.parsing.AvroToSparkParser

class AvroToSparkParserSpec extends FlatSpec {

  private val avroParser = new AvroToSparkParser()

  behavior of "AvroToSparkParser"

  it should "support native types" in {
    val testData = Map[String, Object](
      "string" ->  "A Test String",
      "float" ->   new Float(Float.MAX_VALUE),
      "int" ->     new Integer(Integer.MAX_VALUE),
      "long" ->    new Long(Long.MAX_VALUE),
      "double" ->  new Double(Double.MAX_VALUE),
      "boolean" -> new Boolean(true))

    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.NATIVE_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }
  }

  it should "support union with NULL in native types" in {
    val testData = Map[String, Object](
      "string"  -> null,
      "float"   -> null,
      "int"     -> null,
      "long"    -> null,
      "double"  -> null,
      "boolean" -> null)

    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.NATIVE_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (i <- 0 until resultRow.size) {
      assert(resultRow.get(i) == null)
    }
  }

  it should "support array type" in {
    val testData = Map[String, Object](
      "array" -> new ArrayList(Arrays.asList("elem1", "elem2")))

    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.ARRAY_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }
  }

  it should "support map type" in {
    val map = new HashMap[String, java.util.ArrayList[Long]]()
    map.put("entry1", new ArrayList(java.util.Arrays.asList(new Long(1), new Long(2))))
    map.put("entry2", new ArrayList(java.util.Arrays.asList(new Long(3), new Long(4))))

    val testData = Map[String, Object](
      "map" -> map)

    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.MAP_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }
  }

  it should "support bytes type" in {
    val testData = Map[String, Object](
      "bytes" -> ByteBuffer.wrap("ASimpleString".getBytes))

    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.BYTES_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }
  }

  it should "support fixed type" in {    
    val testData = Map[String, Object](
      "fixed" -> new FixedString("ASimpleString"))

    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.FIXED_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }    
  }
  
  it should "support decimal type" in {    
    val testData = Map[String, Object](
      "decimal" -> ByteBuffer.wrap("1".getBytes))

    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.DECIMAL_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)
    
    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }       
  }
  
  it should "support date type" in {
    val testData = Map[String, Object](
      "date" -> new Integer(Integer.MAX_VALUE))
         
    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.DATE_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }        
  }  

  it should "support millisecond type" in {
    val testData = Map[String, Object](
      "millisecond" -> new Integer(Integer.MAX_VALUE))
         
    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.MILLISECOND_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }        
  }    
  
  it should "support microsecond type" in {
    val testData = Map[String, Object](
      "microsecond" -> new Long(Long.MAX_VALUE))
         
    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.MICROSECOND_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }        
  }      
  
  it should "support timestamp millis type" in {
    val testData = Map[String, Object](
      "timestampMillis" -> new Long(Long.MAX_VALUE))
         
    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.TIMESTAMP_MILLIS_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }        
  }        

  it should "support timestamp micros type" in {
    val testData = Map[String, Object](
      "timestampMicros" -> new Long(Long.MAX_VALUE))
         
    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.TIMESTAMP_MICROS_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }        
  }     
  
  it should "support duration type" in {
    val testData = Map[String, Object](
      "duration" -> new FixedString("111111111111"))
         
    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.DURATION_MICROS_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }        
  }    
  
  it should "convert Avro's GenericRecord to Row using informed NESTED Schema" in {
    case class Street(name: String, zip: String)
    case class Neighborhood(name: String, streets: List[Street])
    case class City(name: String, neighborhoods: Array[Neighborhood])
    case class State(name: String, regions: Map[String, List[City]])

    val street1 = Map("name" -> "first street name",   "zip" -> "140 000-00")
    val street2 = Map("name" -> "second street name",  "zip" -> "240 100-00")
    val street3 = Map("name" -> "third street name",   "zip" -> "340 000-00")
    val street4 = Map("name" -> "fourth street name",  "zip" -> "480 100-00")
    val street5 = Map("name" -> "fifth street name",   "zip" -> "580 100-00")
    val street6 = Map("name" -> "sixth street name",   "zip" -> "680 100-00")
    val street7 = Map("name" -> "seventh street name", "zip" -> "780 100-00")
    val street8 = Map("name" -> "eigth street name",   "zip" -> "880 100-00")

    val neighborhood1 = Map(
      "name" -> "A neighborhood",
      "streets" -> new ArrayList(java.util.Arrays.asList(
        AvroParsingTestUtils.mapToGenericRecord(street1, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(street2, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC))))

    val neighborhood2 = Map(
      "name" -> "B neighborhood",
      "streets" -> new ArrayList(java.util.Arrays.asList(
        AvroParsingTestUtils.mapToGenericRecord(street3, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(street4, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC))))

    val neighborhood3 = Map(
      "name" -> "C neighborhood",
      "streets" -> new ArrayList(java.util.Arrays.asList(
        AvroParsingTestUtils.mapToGenericRecord(street5, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(street6, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC))))

    val neighborhood4 = Map(
      "name" -> "D neighborhood",
      "streets" -> new ArrayList(java.util.Arrays.asList(
        AvroParsingTestUtils.mapToGenericRecord(street7, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(street8, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC))))

    val city1 = Map(
      "name" -> "first city",
      "neighborhoods" -> new ArrayList(java.util.Arrays.asList(
        AvroParsingTestUtils.mapToGenericRecord(neighborhood1, TestSchemas.COMPLEX_SCHEMA_NEIGHBORHOOD_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(neighborhood2, TestSchemas.COMPLEX_SCHEMA_NEIGHBORHOOD_SPEC))))

    val city2 = Map(
      "name" -> "second city",
      "neighborhoods" -> new ArrayList(java.util.Arrays.asList(
        AvroParsingTestUtils.mapToGenericRecord(neighborhood3, TestSchemas.COMPLEX_SCHEMA_NEIGHBORHOOD_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(neighborhood4, TestSchemas.COMPLEX_SCHEMA_NEIGHBORHOOD_SPEC))))

    val cityList = new ArrayList(java.util.Arrays.asList(
        AvroParsingTestUtils.mapToGenericRecord(city1, TestSchemas.COMPLEX_SCHEMA_CITY_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(city2, TestSchemas.COMPLEX_SCHEMA_CITY_SPEC)
                       ))
        
    val cities = new HashMap[String, Object]()
    cities.put("cities", cityList)
        
    val state = Map(
      "name" -> "A State",
      "regions" -> cities)

    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(state, TestSchemas.COMPLEX_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    val cityMap: scala.collection.mutable.HashMap[String,Object] = resultRow.getAs("regions")
    
    for (i <- 0 until 2) {
      val record = cityMap.get("cities").get.asInstanceOf[scala.collection.mutable.ListBuffer[GenericRecord]].get(i)
      assert(cityList.get(i).toString() == record.toString())
    }
  }  
  
  private def assertEquals(original: Any, retrieved: Any) = {
    original match {
      case value: java.util.ArrayList[Object] => retrieved.asInstanceOf[mutable.ListBuffer[Any]].toList == original.asInstanceOf[java.util.ArrayList[Any]].toList
      case value: java.util.HashSet[Object]   => retrieved.asInstanceOf[mutable.ListBuffer[Any]].toList == original.asInstanceOf[java.util.Set[Any]].toList
      case value: java.util.HashMap[String,Object]   => {
        val retrievedMap = retrieved.asInstanceOf[mutable.HashMap[Any, Any]]
        val scalaMap = original.asInstanceOf[java.util.HashMap[Any, java.util.ArrayList[Any]]].asScala
        
        for ((key,value) <- scalaMap.iterator) {
          val seq1 = retrievedMap.get(key).get.asInstanceOf[Seq[Any]].toArray
          val seq2 = value.toArray()
          if (seq1 != seq2) {            
            false
          }
        }
        true
      }
      case value: FixedString => {        
        val str1 = new String(original.asInstanceOf[FixedString].bytes())
        val str2 = new String(retrieved.asInstanceOf[Array[Byte]])          
        str1 == str2
      }
      case value: ScalaAvroRecord => {
        val originalRecord = original.asInstanceOf[ScalaAvroRecord]
        val retrievedRecord = retrieved.asInstanceOf[GenericRow]
        for (i <- 0 until originalRecord.getValues().length) {
          if (originalRecord.get(i) != retrievedRecord.get(i)) {
            false
          }
        }
        true
      }
      case value: ByteBuffer => {                
        val str1 = new String(original.asInstanceOf[ByteBuffer].array())
        val str2 = new String(retrieved.asInstanceOf[Array[Byte]])        
        str1 == str2
      }
      case _ => original == retrieved
    }
  }  
}