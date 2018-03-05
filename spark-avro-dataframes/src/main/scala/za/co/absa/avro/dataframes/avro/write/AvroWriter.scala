package za.co.absa.avro.dataframes.avro.write

import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.Encoder
import org.apache.avro.Schema
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.DatumWriter

class AvroWriter {
   
  def toByteArray(record: IndexedRecord, schema: Schema): Array[Byte] = {
    val outStream = new ByteArrayOutputStream()
    val encoder = getEncoder(outStream)
    try {                 
      getWriter(schema).write(record, encoder)
      encoder.flush()
      outStream.toByteArray()            
    } finally {      
      outStream.close()
    }    
  }
  
  private def getWriter(schema: Schema): DatumWriter[IndexedRecord] = {    
    this.writer.setSchema(schema)
    this.writer
  }
  
  private def getEncoder(outStream: ByteArrayOutputStream): Encoder = {       
    if (encoder == null) {     
     encoder = EncoderFactory.get().binaryEncoder(outStream, null)
    }
    EncoderFactory.get().binaryEncoder(outStream, encoder)
  }
  
  private val writer = new GenericDatumWriter[IndexedRecord]()
  private var encoder: BinaryEncoder = null  
}