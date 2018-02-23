package za.co.absa.avro.dataframes.utils.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

public class AvroPayloadConverter {

	private final Schema schema;
	private final DatumWriter<IndexedRecord> writer;
	
	public AvroPayloadConverter(Schema schema) {
		this.schema = schema;
		this.writer = new GenericDatumWriter<IndexedRecord>(schema);
	}
	
	public final byte[] toAvroPayload(Object o, List<Field> fields) throws IllegalArgumentException, IllegalAccessException, IOException {
		return this.recordToByteArray(this.toRecord(o, fields));
	}
	
	private final IndexedRecord toRecord(Object o, List<Field> fields) throws IllegalArgumentException, IllegalAccessException {
		GenericRecord record = new GenericData.Record(schema);		
		for (Field field : fields) {			
			record.put(field.getName(), field.get(o));
		}
		return record;
	}

	private final byte[] recordToByteArray(IndexedRecord datum) throws IOException {
		
		try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()){
			Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);			
			writer.write(datum, encoder);
			encoder.flush();
			byte[] byteData = outStream.toByteArray();
			return byteData;
		} 
	}
}
