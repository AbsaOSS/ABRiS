package za.co.absa.avro.dataframes.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import za.co.absa.avro.dataframes.examples.utils.ExamplesUtils
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import za.co.absa.avro.dataframes.avro.format.SparkAvroConversions
import za.co.absa.avro.dataframes.examples.data.generation.ComplexRecordsGenerator
import za.co.absa.avro.dataframes.avro.parsing.utils.AvroSchemaUtils
import java.util.Properties
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoder
import java.io.FileInputStream
import org.apache.spark.sql.SaveMode

object SampleAvroParquetExample {

  private val PARQUET_PATH = "testParquetDestination"
  private val AVRO_SCHEMA = "src\\test\\resources\\example_schema.avsc"
  
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("AvroParquetTest")
      .master("local[2]")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("info")

    writeAvroToParquet(PARQUET_PATH, 10, spark)
    val parquetDF = readParquetAsAvro(PARQUET_PATH, spark)
    
    parquetDF.show()
  }

  private def writeAvroToParquet(destination: String, numRecords: Int, spark: SparkSession) = {
    
    import za.co.absa.avro.dataframes.avro.AvroSerDe._
    import spark.implicits._
    import ExamplesUtils._
    
    implicit val encoder = getEncoder()
        
    val rows = getRows(10)
    val dataframe = spark.sparkContext.parallelize(rows, 8).toDF()      
      
    dataframe
      .avro(AVRO_SCHEMA)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destination)      
  }
  
  private def readParquetAsAvro(source: String, spark: SparkSession): Dataset[Row] = {
    
    import spark.implicits._
    import za.co.absa.avro.dataframes.avro.AvroSerDe._
    
    spark
      .read
      .parquet(source)      
      .dataframeToavro(AVRO_SCHEMA)
  }
  
  private def loadProperties(path: String): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream(path))
    properties
  }

  private def getRows(howMany: Int): List[Row] = {
    ComplexRecordsGenerator.generateUnparsedRows(howMany)
  }

  private def getEncoder(): Encoder[Row] = {
    val avroSchema = AvroSchemaUtils.load(AVRO_SCHEMA)
    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)
    RowEncoder.apply(sparkSchema)
  }        
}