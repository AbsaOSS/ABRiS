/*
 * Copyright 2018 Barclays Africa Group Limited
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

package za.co.absa.avro.dataframes.utils.avro;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;

import za.co.absa.avro.dataframes.utils.ReflectionUtils;
import za.co.absa.avro.dataframes.utils.avro.data.annotations.NestedAvroData;

/**
 * Converts objects into Avro IndexedRecords.
 */
public class AvroRecordGenerator {

	public final static Optional<IndexedRecord> convert(Object data) {
		Schema dataSchema = AvroSchemaGenerator.parseSchema(data.getClass());		
		List<Field> dataFields = ReflectionUtils.getAccessibleFields(data.getClass());
		
		try {
			return Optional.of(generate(dataSchema, data, dataFields));
		}
		catch (Exception e) {
			e.printStackTrace();			
			return Optional.empty();
		}
	}
	
	private final static IndexedRecord generate(Schema dataSchema, Object data, List<Field> dataFields) throws IllegalArgumentException, IllegalAccessException {
		GenericRecordBuilder recordBuilder = new GenericRecordBuilder(dataSchema);
		
		for (Field field : dataFields) {

			Object fieldValue = field.get(data);
			if (fieldValue.getClass().isAnnotationPresent(NestedAvroData.class)) {
				fieldValue = convert(fieldValue).get(); // convert the class into an Avro's IndexedRecord
			}
			
			recordBuilder.set(field.getName(), fieldValue);
		}		
		return recordBuilder.build();		
	}
}
