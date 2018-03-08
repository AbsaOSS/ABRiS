package za.co.absa.avro.dataframes.avro

import scala.reflect.ClassTag

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.DecoderFactory
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.DataStreamReader

import za.co.absa.avro.dataframes.avro.format.SparkAvroConversions
import za.co.absa.avro.dataframes.avro.parsing.AvroToSparkParser
import za.co.absa.avro.dataframes.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.avro.dataframes.avro.read.ScalaDatumReader
import java.security.InvalidParameterException

/**
 * This object provides the main point of integration between applications and this library.
 */
object AvroSerDe {

  private val avroParser = new AvroToSparkParser()
  private var reader: ScalaDatumReader[GenericRecord] = _
  private var decoder: BinaryDecoder = _ // allows for object reuse

  /**
   * Method responsible for receiving binary Avro records and converting them into Spark Rows.
   */
  private def decodeAvro[T](avroRecord: Array[Byte])(implicit tag: ClassTag[T]): Row = {
    decoder = DecoderFactory.get().binaryDecoder(avroRecord, decoder)
    val decodedAvroData: GenericRecord = reader.read(null, decoder)

    avroParser.parse(decodedAvroData)
  }

  private def createAvroReader(schema: String) = {
    reader = new ScalaDatumReader[GenericRecord](AvroSchemaUtils.load(schema))
  }

  private def createRowEncoder(schema: Schema) = {
    RowEncoder(SparkAvroConversions.toSqlType(schema))
  }

  /**
   * This class provides the method that performs the Kafka/Avro/Spark connection.
   *
   * It loads binary data from a stream and feed them into an Avro/Spark decoder, returning the resulting rows.
   *
   * It requires the path to the Avro schema which defines the records to be read.
   */
  implicit class Deserializer(dsReader: DataStreamReader) extends Serializable {

    def avro(schema: String) = {

      createAvroReader(schema)

      val rowEncoder = createRowEncoder(reader.getSchema)

      val data = dsReader.load.select("value").as(Encoders.BINARY)      

      val rows = data.map(avroRecord => {
        decodeAvro(avroRecord)
      })(rowEncoder)

      rows
    }
  }
  
  implicit class Serializer(dataframe: Dataset[Row]) {
    
    implicit val recEncoder = Encoders.BINARY
    
    // keeping API since spark changes nullability of schemas when applying encoders, which leads to different avro schemas which leads to unread columns
    // SPARK-14139
    def avro(schemaPath: String): Dataset[Array[Byte]] = {      
      
      val plainAvroSchema = AvroSchemaUtils.loadPlain(schemaPath)            
      toAvro(dataframe, plainAvroSchema)
    }
    
    def avro(schemaName: String, schemaNamespace: String): Dataset[Array[Byte]] = {      

      if (dataframe.schema == null || dataframe.schema.isEmpty) {
        throw new InvalidParameterException("Dataframe does not have a schema.")
      }
      
      val plainAvroSchema = SparkAvroConversions.toAvroSchema(dataframe.schema, schemaName, schemaNamespace).toString()                     
      toAvro(dataframe, plainAvroSchema)
    }   
    
    private def toAvro(rows: Dataset[Row], plainAvroSchema: String) = {
      rows.mapPartitions(partition => {        
        val avroSchema = AvroSchemaUtils.parse(plainAvroSchema)
        val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)
        partition.map(row => SparkAvroConversions.rowToBinaryAvro(row, sparkSchema, avroSchema))        
      })      
    }
  }    
}