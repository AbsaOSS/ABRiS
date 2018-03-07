package za.co.absa.avro.dataframes.avro.write

import org.scalatest.FlatSpec
import java.io.ByteArrayOutputStream
import za.co.absa.avro.dataframes.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.avro.dataframes.utils.TestSchemas

class AvroWriterHolderSpec extends FlatSpec {
  
  behavior of "AvroWriterHolder"
  
  it should "reuse Encoders" in {    
    val holder = new AvroWriterHolder()
    val encoder = holder.getEncoder(new ByteArrayOutputStream())
    assert(encoder == holder.getEncoder(new ByteArrayOutputStream()))
  }
}