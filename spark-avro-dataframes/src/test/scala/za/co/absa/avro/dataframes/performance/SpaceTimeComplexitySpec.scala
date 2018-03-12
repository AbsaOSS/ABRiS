/*
 * Copyright 2018 Barclays Africa Group Limited
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

package za.co.absa.avro.dataframes.performance

import org.scalatest.FlatSpec
import java.io.File
import org.apache.commons.io.FileUtils
import com.sun.xml.internal.bind.v2.schemagen.xmlschema.ComplexRestriction
import za.co.absa.avro.dataframes.examples.data.generation.ComplexRecordsGenerator
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import za.co.absa.avro.dataframes.avro.parsing.utils.AvroSchemaUtils
import org.apache.spark.sql.types.StructType
import com.databricks.spark.avro.SchemaConverters
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory
import za.co.absa.avro.dataframes.examples.data.generation.ComplexRecordsGenerator.Bean
import za.co.absa.avro.dataframes.avro.parsing.AvroToSparkParser
import za.co.absa.avro.dataframes.avro.format.SparkAvroConversions
import java.nio.ByteBuffer
import za.co.absa.avro.dataframes.examples.data.generation.FixedString

class SpaceTimeComplexitySpec extends FlatSpec with BeforeAndAfterAll {

  private val logger = LoggerFactory.getLogger(this.getClass)
  
  private var parentTestDir: File = _
  private val spark: SparkSession = SparkSession
    .builder()
    .appName("SpaceTester")
    .master("local[2]")
    .getOrCreate()

  override def beforeAll() {    
    parentTestDir = new File("test")
    parentTestDir.mkdirs()
  }

  override def afterAll() {    
    spark.close()    
    FileUtils.deleteDirectory(parentTestDir)
  }

  behavior of "Library Performance"
    
  it should "be more space-efficient than Kryo when serializing records" in {    
    val avroSchema = AvroSchemaUtils.parse(ComplexRecordsGenerator.usedAvroSchema)
    val sparkSchema: StructType = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]    
    
    val numRecords = 50000
    val data = ComplexRecordsGenerator.generateRecords(numRecords)
        
    val avroResult = writeAvro(ComplexRecordsGenerator.lazilyConvertToRows(data), sparkSchema)     
    val kryoResult = writeKryo(ComplexRecordsGenerator.lazilyConvertToBeans(data))
    
    val avroSize = FileUtils.sizeOf(avroResult).doubleValue()
    val kryoSize = FileUtils.sizeOf(kryoResult).doubleValue()    
    
    assert(avroSize <= kryoSize)
    
    val spaceSaving = round((1d - avroSize/kryoSize) * 100, 2)  
    val avroSizeKB = round(avroSize/1024, 0).toInt
    val kryoSizeKB = round(kryoSize/1024, 0).toInt
    
    println(s"******* Avro was ${spaceSaving}% more space-efficient than Kryo while writing ${numRecords} rows. (Kryo = ${kryoSizeKB}KB, Avro = ${avroSizeKB}KB)")    
  }

  it should "parse at least 100k records/second/core into Spark Rows" in {        
    val records = ComplexRecordsGenerator.generateRecords(100000)
    val avroParser = new AvroToSparkParser()
    val init = System.nanoTime()
    val numRows = ComplexRecordsGenerator.eagerlyConvertToRows(records).size
    val elapsed = System.nanoTime() - init
    println(s"******* AvroSparkParser processed ${numRows} records in ${elapsed/1000000} ms.")
    assert(elapsed < 1e+9)    
  }  
  
  it should " parse at least 5k rows/second/core into Avro records" in {    
    val avroSchema = AvroSchemaUtils.parse(ComplexRecordsGenerator.usedAvroSchema)
    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)
    val rows = ComplexRecordsGenerator.generateUnparsedRows(5000)    
    val init = System.nanoTime()
    val numRecords = rows.map(row => {      
      SparkAvroConversions.rowToBinaryAvro(row, sparkSchema, avroSchema)
    }).size
    val elapsed = System.nanoTime() - init
    println(s"******* AvroSparkParser processed ${numRecords} rows in ${elapsed/1000000} ms.")
    assert(elapsed < 1e+9)
  }    
  
  private def writeKryo(data: List[Bean]): File = {
    import spark.implicits._
    implicit val encoderAvro = Encoders.kryo[Bean]
    val kryoDir = getKryoDestinationDir()
    val kryoDF = spark.sparkContext.parallelize(data, 1).toDF()
    kryoDF.write.mode(SaveMode.Overwrite).save(kryoDir.getAbsolutePath)
    kryoDir
  }  
  
  private def writeAvro(data: List[Row], schema: StructType): File = {
    import spark.implicits._
    implicit val encoderAvro = RowEncoder.apply(schema)
    val avroDir = getAvroDestinationDir()
    val avroDF = spark.sparkContext.parallelize(data, 1).toDF()
    avroDF.write.mode(SaveMode.Overwrite).save(avroDir.getAbsolutePath)
    avroDir
  }
  
  private def getAvroDestinationDir(): File = {
    getDestinationDir("avro")
  }

  private def getKryoDestinationDir(): File = {
    getDestinationDir("kryo")
  }

  private def getDestinationDir(name: String): File = {
    new File(parentTestDir, name)
  }
  
  private def round(value: Double, scale: Int): Double = {
    BigDecimal(value).setScale(scale, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}