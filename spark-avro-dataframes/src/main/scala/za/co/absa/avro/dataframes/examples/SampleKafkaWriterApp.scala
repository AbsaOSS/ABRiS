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



case class Test(a: Int, b: Long)

object SampleKafkaWriterApp {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("writer")
      .master("local[2]")
      .getOrCreate()  
            
    val sparkSchema = new StructType(Array(new StructField("int",IntegerType, false), new StructField("string",StringType,false), new StructField("array1", new ArrayType(LongType, false), false)))    
    
    implicit val encoder = RowEncoder.apply(sparkSchema)
    
    import spark.implicits._
    
    val l = spark.sparkContext.parallelize(List(Row.fromTuple((1, "the string 5", Seq(1l, 2l))), Row.fromTuple((11, "the string 55", Seq(3l, 4l)))), 2) 
    val l3 = l.toDF()
           
    // STORE THE SCHEMA HERE
    
    import za.co.absa.avro.dataframes.avro.AvroSerDe._
    
    //val l2 = l3.avro(schemaPath)
    val l2 = l3.avro("teste1", "teste2")    
    
    l2.write
    .format("kafka")    
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "avro-dataframes-topic")
    .save()

    l2.foreach(f => {
      f.foreach(print)
      println
    })
    
  }  
}