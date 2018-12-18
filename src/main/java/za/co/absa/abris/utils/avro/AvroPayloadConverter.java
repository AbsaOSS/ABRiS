/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.abris.utils.avro;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import za.co.absa.abris.utils.avro.data.annotations.NestedAvroData;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Optional;

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
