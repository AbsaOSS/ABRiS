package za.co.absa.avro.dataframes.utils.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

import za.co.absa.avro.dataframes.utils.avro.data.NestedAvroData;

public class AvroPayloadConverter {
	
	public final byte[] toAvroPayload(Object o) throws Exception {
		
		Optional<IndexedRecord> record = AvroRecordGenerator.convert(o);
		
		if (record.isPresent()) {
			return recordToByteArray(record.get());
		}
		else {
			throw new IllegalArgumentException("Could not convert "+o.getClass()+" to Avro IndexedRecord.");			
		}
	}

	private final byte[] recordToByteArray(IndexedRecord datum) throws IOException {		
		try (ByteArrayOutputStream outStream = new ByteArrayOutputStream()){
			DatumWriter<IndexedRecord> writer = new GenericDatumWriter<IndexedRecord>(datum.getSchema());
			Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);			
			writer.write(datum, encoder);
			encoder.flush();
			byte[] byteData = outStream.toByteArray();
			return byteData;
		} 
		catch (IOException e) {			
			throw new IOException("Are you using nested classes? If yes, did you mark them as "+NestedAvroData.class.getName(), e);
		}
	}
}
