package za.co.absa.avro.dataframes.utils.examples;

import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import za.co.absa.avro.dataframes.utils.avro.AvroSchemaGenerator;
import za.co.absa.avro.dataframes.utils.avro.kafka.Settings;
import za.co.absa.avro.dataframes.utils.avro.kafka.write.KafkaAvroWriter;
import za.co.absa.avro.dataframes.utils.examples.utils.TestDataGenerator;
import za.co.absa.avro.dataframes.utils.examples.utils.TestDataGenerator.TestData;

/**
 * Writes Avro data to Kafka using the utilities API.
 *
 */
public class SimpleAvroDataGenerator {

	private final static Properties getConfig() {		
		Properties properties = new Properties();
		properties.put("bootstrap.servers", Settings.METADATA_BROKER_LIST);
		properties.put("metadata.broker.list", Settings.METADATA_BROKER_LIST);
		properties.put("client.id", UUID.randomUUID().toString());
		properties.put("acks","all");
		properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
		properties.put("block.on.buffer.full","false");
		return properties;
	}

	private final static void storeSchemaForReader(String destination, Class<?> example) {
		AvroSchemaGenerator.storeSchemaForClass(example, Paths.get(destination));
	}

	public static void main(String[] args) throws Exception {

		String schemaDestination = "src\\test\\resources\\automatically_generated_schema.avsc";
		storeSchemaForReader(schemaDestination, TestDataGenerator.getContainerClass()); // automatically write the schema so that an Avro reader can pick it

		Properties config = getConfig(); // configure metadata for Kafka connection
		KafkaAvroWriter<TestData> writer = new KafkaAvroWriter<TestData>(config); // Writer for a user-defined class

		while (true) {
			List<TestData> testData = TestDataGenerator.generate(10);						
			writer.write(testData, Settings.TOPICS, 1l);
			Thread.sleep(3000);
		}

	}	
}
