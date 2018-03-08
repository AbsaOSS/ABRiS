package za.co.absa.avro.dataframes.examples

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.ArrayType
import za.co.absa.avro.dataframes.examples.data.generation.ComplexRecordsGenerator
import org.apache.spark.sql.Encoder
import za.co.absa.avro.dataframes.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.avro.dataframes.avro.format.SparkAvroConversions

import org.apache.kafka.common.serialization.BytesSerializer
import java.util.Properties
import java.io.FileInputStream

import scala.collection.JavaConversions._
import org.apache.spark.sql.Dataset

object SampleKafkaDataframeWriterApp {
   
  private val PARAM_JOB_NAME = "job.name"
  private val PARAM_JOB_MASTER = "job.master"
  private val PARAM_KAFKA_SERVERS = "kafka.bootstrap.servers"
  private val PARAM_KAFKA_TOPICS = "kafka.topics"
  private val PARAM_AVRO_SCHEMA = "avro.schema"
  private val PARAM_AVRO_RECORD_NAME = "avro.record.name"  
  private val PARAM_AVRO_RECORD_NAMESPACE = "avro.record.namespace"
  private val PARAM_INFER_SCHEMA = "infer.schema"
  private val PARAM_LOG_LEVEL = "log.level"
  private val PARAM_TEST_DATA_ENTRIES = "test.data.entries"
    
  def main(args: Array[String]): Unit = {
    
    if (args.length != 1) {
      println("No properties file specified.")
      System.exit(1)
    }

    println("Loading properties from: " + args(0))
    val properties = loadProperties(args(0))    
    
    val spark = SparkSession
      .builder()
      .appName("writer")
      .master("local[2]")
      .getOrCreate()  
            
      spark.sparkContext.setLogLevel(properties.getProperty(PARAM_LOG_LEVEL))
      
      import za.co.absa.avro.dataframes.avro.AvroSerDe._
      import spark.implicits._
      
      implicit val encoder = getEncoder()
      val dataframe = getRows(properties.getProperty(PARAM_TEST_DATA_ENTRIES).trim().toInt).toDF()
      
      toAvro(dataframe, properties)
      .write
      .format("kafka")    
      .option("kafka.bootstrap.servers", properties.getProperty(PARAM_KAFKA_SERVERS))
      .option("topic", properties.getProperty(PARAM_KAFKA_TOPICS))
      .save()    
  }  
  
  private def toAvro(dataframe: Dataset[Row], properties: Properties) = {
    import za.co.absa.avro.dataframes.avro.AvroSerDe._    
    if (properties.getProperty(PARAM_INFER_SCHEMA).trim().toBoolean) {
      dataframe.avro(properties.getProperty(PARAM_AVRO_RECORD_NAME), properties.getProperty(PARAM_AVRO_RECORD_NAMESPACE))
    }
    else {
      dataframe.avro(properties.getProperty(PARAM_AVRO_SCHEMA))
    }
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
    val avroSchema = AvroSchemaUtils.parse(ComplexRecordsGenerator.usedAvroSchema)
    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)    
    RowEncoder.apply(sparkSchema)
  }  
}