package za.co.absa.avro.dataframes.parsing

import java.lang.Double
import java.lang.Float
import java.lang.Long
import java.util.ArrayList
import java.util.HashMap

import scala.collection._
import scala.collection.JavaConversions._
import scala.collection.immutable.Map

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.scalatest._
import org.scalatest.FlatSpec

import za.co.absa.avro.dataframes.utils.AvroParsingTestUtils
import za.co.absa.avro.dataframes.utils.TestSchemas
import java.util.Arrays
import java.nio.ByteBuffer
import org.apache.avro.generic.GenericRecord

class AvroParserSpec extends FlatSpec {

  private val avroParser = new AvroParser()

  behavior of "AvroParser"

  it should "support native types" in {
    val testData = Map[String, Any](
      "string" -> "A Test String",
      "float" -> Float.MAX_VALUE,
      "int" -> Integer.MAX_VALUE,
      "long" -> Long.MAX_VALUE,
      "double" -> Double.MAX_VALUE,
      "boolean" -> true)

    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.NATIVE_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }
  }

  it should "support union with NULL in native types" in {
    val testData = Map[String, Any](
      "string" -> null,
      "float" -> null,
      "int" -> null,
      "long" -> null,
      "double" -> null,
      "boolean" -> null)

    val avroRecord = AvroParsingTestUtils.mapToGenericRecord(testData, TestSchemas.NATIVE_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (i <- 0 until resultRow.size) {
      assert(resultRow.get(i) == null)
    }
  }

  it should "support array types" in {
    val testData = Map[String, Object](
      "array" -> new ArrayList(Arrays.asList("elem1", "elem2")))

    val avroRecord = AvroParsingTestUtils.mapToCustomRecord(testData, TestSchemas.ARRAY_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }
  }

  it should "support map types" in {
    val map = new HashMap[String, java.util.ArrayList[Long]]()
    map.put("entry1", new ArrayList(java.util.Arrays.asList(new Long(1), new Long(2))))
    map.put("entry2", new ArrayList(java.util.Arrays.asList(new Long(3), new Long(4))))

    val testData = Map[String, Object](
      "array" -> new ArrayList(Arrays.asList("elem1", "elem2")))

    val avroRecord = AvroParsingTestUtils.mapToCustomRecord(testData, TestSchemas.ARRAY_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    for (entry <- testData) {
      assert(assertEquals(entry._2, resultRow.getAs(entry._1)), s"${entry._1} did not match")
    }
  }

  it should "support bytes types" in {
    val testData = Map[String, Object](
      "bytes" -> ByteBuffer.wrap("ASimpleString".getBytes))

    val avroRecord = AvroParsingTestUtils.mapToCustomRecord(testData, TestSchemas.BYTES_SCHEMA_SPEC)
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

    val street1 = Map("name" -> "first street name", "zip" -> "140 000-00")
    val street2 = Map("name" -> "second street name", "zip" -> "240 100-00")
    val street3 = Map("name" -> "third street name", "zip" -> "340 000-00")
    val street4 = Map("name" -> "fourth street name", "zip" -> "480 100-00")
    val street5 = Map("name" -> "fifth street name", "zip" -> "580 100-00")
    val street6 = Map("name" -> "sixth street name", "zip" -> "680 100-00")
    val street7 = Map("name" -> "seventh street name", "zip" -> "780 100-00")
    val street8 = Map("name" -> "eigth street name", "zip" -> "880 100-00")

    val neighborhood1 = Map(
      "name" -> "A neighborhood",
      "streets" -> List(
        AvroParsingTestUtils.mapToGenericRecord(street1, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(street2, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC)))

    val neighborhood2 = Map(
      "name" -> "B neighborhood",
      "streets" -> List(
        AvroParsingTestUtils.mapToGenericRecord(street3, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(street4, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC)))

    val neighborhood3 = Map(
      "name" -> "C neighborhood",
      "streets" -> List(
        AvroParsingTestUtils.mapToGenericRecord(street5, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(street6, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC)))

    val neighborhood4 = Map(
      "name" -> "D neighborhood",
      "streets" -> List(
        AvroParsingTestUtils.mapToGenericRecord(street7, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(street8, TestSchemas.COMPLEX_SCHEMA_STREET_SPEC)))

    val city1 = Map(
      "name" -> "first city",
      "neighborhoods" -> List(
        AvroParsingTestUtils.mapToGenericRecord(neighborhood1, TestSchemas.COMPLEX_SCHEMA_NEIGHBORHOOD_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(neighborhood2, TestSchemas.COMPLEX_SCHEMA_NEIGHBORHOOD_SPEC)))

    val city2 = Map(
      "name" -> "second city",
      "neighborhoods" -> List(
        AvroParsingTestUtils.mapToGenericRecord(neighborhood3, TestSchemas.COMPLEX_SCHEMA_NEIGHBORHOOD_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(neighborhood4, TestSchemas.COMPLEX_SCHEMA_NEIGHBORHOOD_SPEC)))

    val cityList = List(
        AvroParsingTestUtils.mapToGenericRecord(city1, TestSchemas.COMPLEX_SCHEMA_CITY_SPEC),
        AvroParsingTestUtils.mapToGenericRecord(city2, TestSchemas.COMPLEX_SCHEMA_CITY_SPEC)
                       )
        
    val cities = new HashMap[String, Object]()
    cities.put("cities", cityList)
        
    val state = Map(
      "name" -> "A State",
      "regions" -> cities)

    val avroRecord = AvroParsingTestUtils.mapToCustomRecord(state, TestSchemas.COMPLEX_SCHEMA_SPEC)
    val resultRow = avroParser.parse(avroRecord)

    val cityMap: scala.collection.mutable.HashMap[String,Object] = resultRow.getAs("regions")
    
    for (i <- 0 until 2) {
      val record = cityMap.get("cities").get.asInstanceOf[scala.collection.mutable.ListBuffer[GenericRecord]].get(i)
      assert(cityList.get(i).toString() == record.toString())
    }
  }

  private def assertEquals(original: Any, retrieved: Any) = {
    original.getClass().getName match {
      case "java.util.ArrayList" => retrieved.asInstanceOf[mutable.ListBuffer[Any]].toList == original.asInstanceOf[java.util.ArrayList[Any]].toList
      case "java.util.HashSet"   => retrieved.asInstanceOf[mutable.ListBuffer[Any]].toList == original.asInstanceOf[java.util.Set[Any]].toList
      case "java.util.HashMap"   => retrieved.asInstanceOf[mutable.HashMap[Any, Any]] == original.asInstanceOf[java.util.HashMap[Any, Any]].toMap
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