package za.co.absa.avro.dataframes.utils

import java.io.ByteArrayOutputStream
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import za.co.absa.avro.dataframes.parsing.ScalaDatumReader
import za.co.absa.avro.dataframes.avro.CustomDatumWriter
import za.co.absa.avro.dataframes.parsing.ScalaRecord

object AvroParsingTestUtils {

  // MAPEAR PARA CUSTOM
  def mapToGenericRecord(data: Map[String, Any], schema: String): GenericRecord = {
    val avroRecordBuilder = getRecordBuilder(schema)
    for (entry <- data.iterator) {
      avroRecordBuilder.set(entry._1, entry._2)
    }
    val avroRecord = avroRecordBuilder.build()
    passRecordThroughAvroApi(avroRecord) // so that we have the proper data types added to each record field
  }

  def mapToCustomRecord(data: Map[String, Object], schema: String): GenericRecord = {    
    val avroRecord = new ScalaRecord(parseSchema(schema))        
    for (entry <- data.iterator) {
      avroRecord.put(entry._1, entry._2)
    }    
    passRecordThroughAvroApi(avroRecord) // so that we have the proper data types added to each record field
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

  private def recordToBytes(record: GenericRecord): Array[Byte] = {
    //	  val writer = new GenericDatumWriter[GenericRecord](record.getSchema);
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

  private def bytesToRecord(avroBytes: Array[Byte], schema: Schema): GenericRecord = {
    val reader = new ScalaDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(avroBytes, null)
    reader.read(null, decoder)
  }

  val PLAIN_SCHEMA_SPEC = """{
     "namespace": "all-types.test",
     "type": "record",
     "name": "testdata",
     "fields":[                  
         {"name": "string",      "type": "string"                                                                 },     
         {"name": "int",         "type": "int"                                                                    },
         {"name": "long",        "type": "long"                                                                   },
 		     {"name": "double",      "type": "double"                                                                 },
 		     {"name": "float",       "type": "float"                                                                  },
 		     {"name": "short",       "type": "int"                                                                    },
 		     {"name": "byte",        "type": "int"                                                                    },    
         {"name": "anyOption",   "type": ["string", "null"]                                                       },
         {"name": "seqAny",      "type": {"type": "array", "items": "string"}                                     },
         {"name": "mapAny",      "type": {"type": "map",   "values": "long" }                                     },
         {"name": "setAny",      "type": {"type": "array", "items": "string"}                                     },
         {"name": "date",        "type": {"type": "int", "logicalType": "date"}                                   },                
         {"name": "timestamp",   "type": {"type": "long", "logicalType": "timestamp-millis"}                      },         
         {"name": "bigDecimal",  "type": {"type": "string", "logicalType": "decimal", "precision": 4, "scale": 20}},
         {"name": "bigInteger",  "type": {"type": "string", "logicalType": "decimal", "precision": 4, "scale": 20}}	 
     ]
  }"""

  val COMPLEX_SCHEMA_SPEC = """{    
     "name": "person",
     "type": "record",
     "fields": [
        {"name": "firstname", "type": "string"},
        {"name": "lastname",  "type": "string"},
        {"name": "addressRecord", "type":
                                  {
                                    "type":   "record",
                                    "name":   "address",
                                    "fields": [
                                        {"name": "streetaddress",     "type": "string"},
                                        {"name": "city",              "type": "string"},
                                        {"name": "previousAddresses", "type": {"type": "array", "items": "string"}}                                    
                                              ]
                                  }
        }
     ]
  }"""
}