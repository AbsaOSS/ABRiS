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

package za.co.absa.abris.utils.avro;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides an utility method for generating Avro schemas from
 * Java class definitions and storing them at specified locations.
 *
 */
public final class AvroSchemaGenerator {
	
	private final static Logger logger = LoggerFactory.getLogger(AvroSchemaGenerator.class);
	
	/**
	 * Generates an Avro schema from clazz's class definition and stores it at destination.
	 * 
	 * @return Optional<Path> if operation was successful or Optional<empty> otherwise.
	 */
	public final static Optional<Path> storeSchemaForClass(Class<?> clazz, Path destination) {		
		Objects.requireNonNull(destination, "Null schema destination.");
		if (!Files.exists(destination.getParent())) {			
			logger.error("Inexistent destination directory: "+destination.getParent().getFileName());
			return Optional.empty();
		}
		
		Schema schema = parseSchema(clazz);		
		if (!storeSchema(schema.toString(), destination)) {
			return Optional.empty();
		}
		
		logger.info("Schema for "+clazz.getName()+" stored in "+destination);
		return Optional.of(destination);
	}

	public final static Schema parseSchema(Class<?> clazz) {		
		Objects.requireNonNull(clazz, "Null template class.");		
		return ReflectData.get().getSchema(clazz);
	}
	
	private final static boolean storeSchema(String schema, Path destination) {		
		try (BufferedWriter writer = Files.newBufferedWriter(destination)) {
			writer.write(schema);			
			return true;
		}
		catch (Exception e) {	
			e.printStackTrace();
			return false;
		}
	}
}
