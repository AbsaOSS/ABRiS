package za.co.absa.avro.dataframes.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class ReflectionUtils {
	
	public final static List<Field> getAccessibleFields(Class<?> clazz) {		
		List<Field> accessibleFields = new ArrayList<Field>();
		for (Field field : clazz.getDeclaredFields()) {
			field.setAccessible(true);
			accessibleFields.add(field);
		}
		return accessibleFields;
	}	
}
