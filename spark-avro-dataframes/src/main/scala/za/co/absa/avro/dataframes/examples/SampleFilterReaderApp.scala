package za.co.absa.avro.dataframes.examples

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import java.io.File

object SampleFilterReaderApp {

  private val PARAM_JOB_NAME = "job.name"
  private val PARAM_JOB_MASTER = "job.master"
  private val PARAM_KAFKA_SERVERS = "kafka.bootstrap.servers"
  private val PARAM_KAFKA_TOPICS = "kafka.topics"
  private val PARAM_AVRO_SCHEMA = "avro.schema"
  private val PARAM_TASK_FILTER = "task.filter"

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("No properties file specified.")
      System.exit(1)
    }    
    
    println("Loading properties from: " + args(0))
    val properties = loadProperties(args(0))
    
    for (key <- properties.keysIterator) {
      println(s"\t${key} = ${properties.getProperty(key)}")
    }
    
    val spark = SparkSession
      .builder()
      .appName(properties.getProperty(PARAM_JOB_NAME))
      .master(properties.getProperty(PARAM_JOB_MASTER))
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
      
    import za.co.absa.avro.dataframes.avro.AvroSerDe._

    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", properties.getProperty(PARAM_KAFKA_SERVERS))
      .option("subscribe", properties.getProperty(PARAM_KAFKA_TOPICS))
      .avro(properties.getProperty(PARAM_AVRO_SCHEMA))

    val filter = properties.getProperty(PARAM_TASK_FILTER)
    println("Going to run filter: " + filter)

    stream.printSchema()
    
    stream
      .filter(filter)
      .writeStream
      .format("console")
      .start()
      .awaitTermination()
  }

  private def loadProperties(path: String): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream(path))
    properties
  }
}