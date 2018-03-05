package za.co.absa.avro.dataframes.examples



import java.io.ByteArrayOutputStream



import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType

import com.databricks.spark.avro.AvroOutputWriter
import com.databricks.spark.avro.SchemaConverters

import za.co.absa.avro.dataframes.utils.avro.AvroPayloadConverter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.IndexedRecord


case class Test(a: Int, b: Long)

object SampleKafkaWriterApp {
    
  val ss = """{
    "namespace": "kakfa-avro.test",
     "type": "record",
     "name": "Test",
     "fields":[
         {  "name": "a", "type": "int"},
         {  "name": "b",  "type": "long"}
      ]}"""
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("writer")
      .master("local[2]")
      .getOrCreate()  
              
    //import spark.implicits._  s
    
   val schemaPath = "C:\\Users\\melofeli\\eclipse-workspace\\spark-avro-dataframes\\spark-avro-dataframes\\src\\test\\resources\\a_schema.avsc"
      
    val avroSchema =  new Schema.Parser().parse(ss)          
    val sparkSchema: StructType = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
    
//    implicit val testEncoder = Encoders.kryo[Test]
    implicit val encoder = RowEncoder.apply(sparkSchema)
//    implicit val recEncoder = Encoders.kryo[GenericRecord]
    
    import spark.implicits._
    
    val l = spark.sparkContext.parallelize(List(Row.fromTuple((1,2l)), Row.fromTuple((3,4l))), 2) 
    val l3 = l.toDF()
    
//    val l2 = l3.mapPartitions(partition => {      
//      val avroLocalSchema =  new Schema.Parser().parse(ss)      
//      partition.map(row => rowToAvro(sparkSchema, avroLocalSchema, row))
//    })
       
    import za.co.absa.avro.dataframes.avro.AvroSerDe._
    
    val l2 = l3.avro(schemaPath)
    
    l2.write
    .format("kafka")    
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("topic", "avro-dataframes-topic")
    .save()

  }  
}