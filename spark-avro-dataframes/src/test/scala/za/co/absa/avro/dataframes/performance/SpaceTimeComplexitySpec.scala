package za.co.absa.avro.dataframes.performance

import org.scalatest.FlatSpec
import java.io.File
import org.apache.commons.io.FileUtils
import com.sun.xml.internal.bind.v2.schemagen.xmlschema.ComplexRestriction
import za.co.absa.avro.dataframes.utils.generation.ComplexRecordsGenerator
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

  behavior of "Library"

  it should "be more space-efficient than Kryo when serializing records" in {    
    val avroSchema = AvroSchemaUtils.parse(ComplexRecordsGenerator.usedSchema)
    val sparkSchema: StructType = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]    
    
    val numRecords = 20000
    val data = ComplexRecordsGenerator.generateRows(numRecords)
        
    val avroResult = writeAvro(data, sparkSchema)     
    val kryoResult = writeKryo(data)
    
    val avroSize = FileUtils.sizeOf(avroResult).doubleValue()
    val kryoSize = FileUtils.sizeOf(kryoResult).doubleValue()    
    
    assert(avroSize <= kryoSize)
    
    val spaceSaving = 1d - avroSize/kryoSize    
    println(s"******* Avro was ${spaceSaving}% more space-efficient than Kryo while writing ${numRecords} rows.")
  }

  it should "process at least 100k rows/second/core" in {        
    val records = ComplexRecordsGenerator.generateRecords(100000)
    val init = System.nanoTime()
    val rows = ComplexRecordsGenerator.convert(records)
    val elapsed = System.nanoTime() - init
    println(s"******* AvroSparkParser processed ${rows.size} records in ${elapsed} ns.")
    assert(elapsed < 1e+9)    
  }
  
  private def writeKryo(data: List[Row]): File = {
    import spark.implicits._
    implicit val encoderAvro = Encoders.kryo[Row]
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
}