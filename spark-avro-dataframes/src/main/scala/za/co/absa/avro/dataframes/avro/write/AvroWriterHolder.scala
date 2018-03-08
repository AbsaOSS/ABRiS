package za.co.absa.avro.dataframes.avro.write

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory

/**
 * Holds an Avro writer. Convenient for encapsulating logic behind object reuse.
 */
class AvroWriterHolder {
     
  def getWriter(schema: Schema): DatumWriter[IndexedRecord] = {    
    this.writer.setSchema(schema)
    this.writer
  }
  
  def getEncoder(outStream: ByteArrayOutputStream): Encoder = {       
    if (encoder == null) {     
     encoder = EncoderFactory.get().binaryEncoder(outStream, null)
    }
    EncoderFactory.get().binaryEncoder(outStream, encoder)
  }
    
  private val writer = new ScalaCustomDatumWriter[IndexedRecord]()
  private var encoder: BinaryEncoder = null  
}