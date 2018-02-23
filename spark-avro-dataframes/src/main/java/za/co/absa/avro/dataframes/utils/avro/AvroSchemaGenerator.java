package za.co.absa.avro.dataframes.utils.avro;

import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

/**
 * This class provides an utility method for generating Avro schemas from
 * Java class definitions and storing them at specified locations.
 *
 */
public final class AvroSchemaGenerator {
	
	/**
	 * Generates an Avro schema from clazz's class definition and stores it at destination.
	 * 
	 * @return Optional<Path> if operation was successful or Optional<empty> otherwise.
	 */
	public final Optional<Path> storeSchemaForClass(Class<?> clazz, Path destination) {		
		Objects.requireNonNull(destination, "Null schema destination.");
		if (!Files.exists(destination.getParent())) {
			System.out.println("Inexistent destination directory: "+destination.getParent().getFileName());
			return Optional.empty();
		}
		
		Schema schema = this.parseSchema(clazz);
		if (!this.storeSchema(schema.toString(), destination)) {
			Optional.empty();
		}
		
		System.out.println("Schema for "+clazz.getName()+" stored in "+destination);
		return Optional.of(destination);
	}

	public final Schema parseSchema(Class<?> clazz) {		
		Objects.requireNonNull(clazz, "Null template class.");		
		return ReflectData.get().getSchema(clazz);
	}
	
	private final boolean storeSchema(String schema, Path destination) {
		
		try (BufferedWriter writer = Files.newBufferedWriter(destination)) {
			writer.write(schema);
			writer.flush();
			return true;
		}
		catch (Exception e) {	
			e.printStackTrace();
			return false;
		}
	}
}
