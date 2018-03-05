package za.co.absa.avro.dataframes.utils

import java.io.ByteArrayOutputStream
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import za.co.absa.avro.dataframes.avro.read.ScalaDatumReader
import za.co.absa.avro.dataframes.utils.avro.CustomDatumWriter


object AvroParsingTestUtils {

  def mapToGenericRecordDirectly(data: Map[String, Object], schema: String): GenericRecord = {
    val avroRecordBuilder = getRecordBuilder(schema)
    for (entry <- data.iterator) {     
      avroRecordBuilder.set(entry._1, entry._2)
    }
    avroRecordBuilder.build()    
  }  
    
  def mapToGenericRecord(data: Map[String, Object], schema: String): GenericRecord = {
    passRecordThroughAvroApi(mapToGenericRecordDirectly(data, schema)) // so that we have the proper data types added to each record field
  }
  
  def getRecordBuilder(schema: String): GenericRecordBuilder = {
    val parsedSchema = parseSchema(schema)
    new GenericRecordBuilder(parsedSchema)
  }

  def parseSchema(schemaSpec: String): Schema = {
    new Schema.Parser().parse(schemaSpec)
  }

  private def passRecordThroughAvroApi(avroRecord: GenericRecord): GenericRecord = {
    val recordBytes = recordToBytes(avroRecord)
    bytesToRecord(recordBytes, avroRecord.getSchema)
  }

  def recordToBytes(record: GenericRecord): Array[Byte] = {
    val writer = new CustomDatumWriter[GenericRecord](record.getSchema);
    val outStream = new ByteArrayOutputStream()
    try {
      val encoder = EncoderFactory.get().binaryEncoder(outStream, null);
      writer.write(record, encoder);
      encoder.flush();
      outStream.toByteArray();
    } finally {
      outStream.close();
    }
  }

  def bytesToRecord(avroBytes: Array[Byte], schema: Schema): GenericRecord = {
    val reader: ScalaDatumReader[GenericRecord] = new ScalaDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(avroBytes, null)
    reader.read(null, decoder)
  }
}