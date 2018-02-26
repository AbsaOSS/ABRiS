package za.co.absa.avro.dataframes.utils.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.avro.Schema;

import za.co.absa.avro.dataframes.utils.avro.AvroSchemaGenerator;
import za.co.absa.avro.dataframes.utils.avro.data.ContainerAvroData;
import za.co.absa.avro.dataframes.utils.avro.data.NestedAvroData;
import za.co.absa.avro.dataframes.utils.avro.kafka.Settings;
import za.co.absa.avro.dataframes.utils.avro.kafka.write.KafkaAvroWriter;

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

	private final static Schema getSchemaFor(Class<?> clazz) {
		return new AvroSchemaGenerator().parseSchema(clazz);
	}

	private final static List<TestBean> getTestData() {
		SimpleNested nested = new SimpleNested(9, "simple name");
		ComplexNested compleNested = new ComplexNested(8l, nested);
		
		Map<String,Integer> map = new HashMap<String,Integer>();
		map.put("k1", 2);
		map.put("k2", 1);

		List<TestBean> testBeans = new ArrayList<TestBean>();
		testBeans.add(new TestBean(1, 2f, 3l, 4d, "s5", 
				Arrays.asList(8l, 9l), 
				new HashSet<Long>(Arrays.asList(10l, 11l)),  
				map, 
				compleNested));

		return testBeans;
	}

	public static void main(String[] args) throws Exception {
		Properties config = getConfig();				
		System.out.println(new AvroSchemaGenerator().parseSchema(TestBean.class));
		KafkaAvroWriter<TestBean> writer = new KafkaAvroWriter<TestBean>(config);

		List<TestBean> list = getTestData();		
		for (int i = 0 ; i < 2 ; i++) {
			writer.write(list, Settings.TOPICS, 2l);
		}
	}	

	private final static class TestBean implements ContainerAvroData {
		private int anInt;
		private float aFloat;
		private long aLong;
		private double aDouble;
		private String aString;
		private List<Long> aList;
		private Set<Long> aSet;
		private Map<String,Integer> mapAny;		
		private ComplexNested nested;
		public TestBean(int anInt, float aFloat, long aLong, double aDouble, String aString, List<Long> aList,
				Set<Long> aSet, Map<String, Integer> aMap, ComplexNested nested) {			
			this.anInt = anInt;
			this.aFloat = aFloat;
			this.aLong = aLong;
			this.aDouble = aDouble;
			this.aString = aString;
			this.aList = aList;
			this.aSet = aSet;
			this.mapAny = aMap;			
			this.nested = nested;
		}	
	}	
	
	private final static class ComplexNested implements NestedAvroData {
		private long whateverLong;
		private SimpleNested nested;
		public ComplexNested(long whateverLong, SimpleNested nested) {		
			this.whateverLong = whateverLong;
			this.nested = nested;
		}		
	}
	
	private final static class SimpleNested implements NestedAvroData {
		private int id;
		private String name;		
		public SimpleNested(int id, String name) {		
			this.id = id;
			this.name = name;
		}		
	}
}
