package za.co.absa.avro.dataframes.utils.avro;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;

import za.co.absa.avro.dataframes.utils.ReflectionUtils;
import za.co.absa.avro.dataframes.utils.avro.data.NestedAvroData;

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
			if (fieldValue instanceof NestedAvroData) {
				fieldValue = convert(fieldValue).get(); // convert the class into an Avro's IndexedRecord
			}
			
			recordBuilder.set(field.getName(), fieldValue);
		}		
		return recordBuilder.build();		
	}
}
