package za.co.absa.avro.dataframes.utils.generation

import java.lang.Boolean
import java.lang.Double
import java.lang.Float
import java.lang.Long
import java.nio.ByteBuffer
import java.util.ArrayList
import java.util.Arrays

import scala.collection._
import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.util.Random

import org.apache.spark.sql.Row

import za.co.absa.avro.dataframes.avro.format.ScalaAvroRecord
import za.co.absa.avro.dataframes.avro.parsing.AvroToSparkParser
import za.co.absa.avro.dataframes.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.avro.dataframes.utils.TestSchemas
import za.co.absa.avro.dataframes.utils.avro.fixed.FixedString
import za.co.absa.avro.dataframes.utils.AvroParsingTestUtils
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.generic.GenericRecord

object ComplexRecordsGenerator {

  case class Bean(bytes: Array[Byte], string: String, int: Int, long: Long, double: Double,
                  float: Float, boolean: Boolean, array: mutable.ListBuffer[Any], fixed: Array[Byte],
                  map: Map[String, java.util.ArrayList[Long]])

  private val plainSchema = TestSchemas.NATIVE_COMPLETE_SCHEMA
  private val avroParser = new AvroToSparkParser()
  private val random = new Random()

  def usedAvroSchema = plainSchema

  def generateRecords(howMany: Int): List[GenericRecord] = {
    val result = new Array[GenericRecord](howMany)
    for (i <- 0 until howMany) {
      result(i) = AvroParsingTestUtils.mapToGenericRecord(getDataMap(), plainSchema)
    }
    result.toList
  }

  def generateUnparsedRows(howMany: Int): List[Row] = {
    val result = new Array[Row](howMany)
    for (i <- 0 until howMany) {
      result(i) = Row.fromSeq(getDataSeq())
    }
    result.toList
  }

  private def getDataMap(): immutable.Map[String, Object] = {
    val map = Map[String, java.util.ArrayList[Long]](
      "entry1" -> randomListOfLongs(20),
      "entry2" -> randomListOfLongs(30))

    immutable.Map[String, Object](
      "bytes" -> ByteBuffer.wrap(randomString(20).getBytes),
      "string" -> randomString(30),
      "int" -> new Integer(random.nextInt()),
      "long" -> new Long(random.nextLong()),
      "double" -> new Double(random.nextDouble()),
      "float" -> new Float(random.nextFloat()),
      "boolean" -> new Boolean(random.nextBoolean()),
      "array" -> randomListOfStrings(10, 15),      
      "map" -> map.asJava,
      "fixed" -> new FixedString(randomString(40)))
  }

  private def getDataSeq(): Seq[Object] = {
    val map = Map[String, Seq[Long]](
      "entry1" -> randomSeqOfLongs(20),
      "entry2" -> randomSeqOfLongs(30))
    Seq(
      ByteBuffer.wrap(randomString(20).getBytes).array(),
      randomString(30),
      new Integer(random.nextInt()),
      new Long(random.nextLong()),
      new Double(random.nextDouble()),
      new Float(random.nextFloat()),
      new Boolean(random.nextBoolean()),
      randomSeqOfStrings(10, 15),            
      map,
      new FixedString(randomString(40)).bytes())
  }

  def lazilyGenerateRows(howMany: Int): List[Row] = {
    lazilyConvertToRows(generateRecords(howMany))
  }

  def eagerlyGenerateRows(howMany: Int): List[Row] = {
    eagerlyConvertToRows(generateRecords(howMany))
  }

  def convertToBeans(records: List[GenericRecord]): List[Bean] = {
    records.toStream.map(record => recordToBean(record)).toList
  }

  def eagerlyConvertToRows(records: List[GenericRecord]): List[Row] = {
    records.map(record => avroParser.parse(record))
  }

  def lazilyConvertToRows(records: List[GenericRecord]): List[Row] = {
    records.toStream.map(record => avroParser.parse(record)).toList
  }

  private def randomListOfLongs(listSize: Int) = {
    val array = new Array[Long](listSize)
    for (i <- 0 until listSize) {
      array(i) = random.nextLong()
    }
    new java.util.ArrayList(array.toList.asJava)
  }
  
  private def randomSeqOfLongs(listSize: Int) = {
    randomListOfLongs(listSize).asScala.toSeq
  }
  
  private def randomListOfStrings(listSize: Int, stringLength: Int) = {
    val array = new Array[String](listSize)
    for (i <- 0 until listSize) {
      array(i) = randomString(stringLength)
    }
    new java.util.ArrayList(array.toList.asJava)
  }

  private def randomSeqOfStrings(listSize: Int, stringLength: Int) = {
    randomListOfStrings(listSize, stringLength).asScala.toSeq    
  }  
  
  private def randomString(length: Int): String = {
    val randomStream: Stream[Char] = Random.alphanumeric
    randomStream.take(length).mkString
  }

  private def recordToBean(record: GenericRecord): Bean = {
    new Bean(
      record.get("bytes").asInstanceOf[Array[Byte]],
      record.get("string").asInstanceOf[String],
      record.get("int").asInstanceOf[Int],
      record.get("long").asInstanceOf[Long],
      record.get("double").asInstanceOf[Double],
      record.get("float").asInstanceOf[Float],
      record.get("boolean").asInstanceOf[Boolean],
      record.get("array").asInstanceOf[mutable.ListBuffer[Any]],
      record.get("fixed").asInstanceOf[Array[Byte]],
      record.get("map").asInstanceOf[Map[String, java.util.ArrayList[Long]]])
  }
}