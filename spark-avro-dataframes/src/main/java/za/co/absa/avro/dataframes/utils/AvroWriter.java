package za.co.absa.avro.dataframes.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class AvroWriter {

//	public static void main(String[] args) throws Exception {
//
////		Schema schema = createSchema(getSchemaFromResources("kafka-avro-test.avsc"));
//		Schema schema = createSchema(getSchemaFromResources("complete_avro_schema.avsc"));		
//		DatumWriter<GenericRecord> writer = createDatumWriter(schema);
//		KafkaProducer<String,byte[]> sender = createKafkaProducer();
//
//		boolean json = false;
//		sendBeans(schema, writer, sender, json);
////		sendUsers(schema, writer, sender);
//	}
//
//	@SuppressWarnings("serial")
//	private final static void sendBeans(Schema schema, DatumWriter<GenericRecord> writer, KafkaProducer<String,byte[]> sender, boolean json) {
//
//		//		 {  "name": "enumeration", "type": {"type":"enum", "name":"enumeration", "symbols" : ["SPADES", "HEARTS"]}},
//		//		 {  "name": "arrayAny",    "type": { "type": "array", "items": "string"} },		
//		//       {  "name": "byteArray",   "type": { "type": "array", "items": "bytes"}  },		
//		//       {  "name": "bigInt",      "type": "double"                              },		
////		         {  "name": "decimal",     "type": {"type": "string", "logicalType": "decimal", "precision": 4, "scale": 20}},
//
//		Random rand = new Random();
//		while (true) {
//			AllDataTypesBean bean = new AllDataTypesBeanBuilder()
//					.anyNullable(null)
//					.anyOption(Optional.of("optionString"))
//					.fixedValue(ByteBuffer.wrap("A fixed value that must be capped".getBytes()))
//					//				.arrayAny(new Object[] {"S1", "S2"})
//					.bigDecimal(new BigDecimal(new BigInteger("888888888888")))
//					.bigInteger(new BigInteger("88888888888888"))
//					.booleanValue(true)
//					.byteValue((byte)2)
//					//				.byteArray(new byte[] {(byte)1, (byte)2})
//					.date(Integer.MAX_VALUE)
////					.decimal(Decimal.)
//					.doubleValue(8.8)
//					//				.enumeration(AllDataTypesBean.CARDS)
//					.floatValue(7.7f)
//					.intValue(6)
//					.mapAny(Collections.unmodifiableMap(new HashMap<String,Long>(){{put("k1",rand.nextLong());put("k2",rand.nextLong());}}))
//					.seqAny(Arrays.asList("e1","e2","e3"))
//					.setAny(new HashSet<String>(Arrays.asList("f1","s2")))
//					.shortValue((short)3)
//					.string("a string")
//					.timestamp(new Timestamp(System.currentTimeMillis()))
//					.nested(new NestedStruct("key", new OneMore(8)))
//					.build();
//
//			System.out.println("Created bean: "+bean);
//
//			GenericRecord record = beanToRecord(bean, schema);
//			if (json) {				
//				sendJson(schema, GlobalSettings.SHARED_TOPICS, record, sender, writer);
//				System.out.println("\tSent Json: " + bean);								
//			}
//			else {
//				send(GlobalSettings.SHARED_TOPICS, record, sender, writer);
//				System.out.println("\tSent binary with anyOption = " + bean.getAnyOption().get());
//			}			
//			
//			try {
//				Thread.sleep(3000);
//			} catch (InterruptedException e) {				
//				e.printStackTrace();
//			}
//		}
//	}
//
//	private final static void sendUsers(Schema schema, DatumWriter<GenericRecord> writer, KafkaProducer<String,byte[]> sender) {
//
//		int usersGeneratedSoFar = 0;
//		int maxRecordsPerIteration = 10;
//
//		Random random = new Random();
//
//		while (true) {
//
//			int numberOfRecords = random.nextInt(maxRecordsPerIteration);                     
//
//			System.out.println("Going to send: "+numberOfRecords+" users.");
//			for (int i = 0 ; i < numberOfRecords ; i++) {            	         
//				User user = createUser(++usersGeneratedSoFar, random);
//				GenericRecord record = userToRecord(user, schema);
//				send(GlobalSettings.SHARED_TOPICS, record, sender, writer);
//				System.out.println("\tSent: " + user);
//			}
//
//			try {
//				Thread.sleep(3000);
//			} 
//			catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}  		
//	}
//	
//	private final static void send(String topics, GenericRecord datum, KafkaProducer<String,byte[]> sender, DatumWriter<GenericRecord> writer) {
//		ProducerRecord<String, byte[]> record = null;                
//		try {
//			record = new ProducerRecord<String, byte[]>(topics, datumToByteArray(writer, datum));
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		sender.send(record);		
//	}
//
//	private final static void sendJson(Schema schema, String topics, GenericRecord datum, KafkaProducer<String,byte[]> sender, DatumWriter<GenericRecord> writer) {
//		ProducerRecord<String, byte[]> record = null;                
//		try {
//			record = new ProducerRecord<String, byte[]>(topics, datumToJson(schema, writer, datum));
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		sender.send(record);		
//	}	
//	
//	public static byte[] datumToByteArray(DatumWriter<GenericRecord> writer, GenericRecord datum) throws IOException {
//				
//		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
//		try {
//			Encoder encoder = EncoderFactory.get().binaryEncoder(outStream, null);
//			writer.write(datum, encoder);
//			encoder.flush();
//			byte[] byteData = outStream.toByteArray();
//			return byteData;
//		} 
//		finally {
//			outStream.close();
//		}
//	}	
//
//	public static byte[] datumToJson(Schema schema, DatumWriter<GenericRecord> writer, GenericRecord datum) throws IOException {
//		
//		ByteArrayOutputStream outStream = new ByteArrayOutputStream();						
//		try {
//			Encoder encoder = EncoderFactory.get().jsonEncoder(schema, outStream);
//			writer.write(datum, encoder);
//			encoder.flush();
//			byte[] byteData = outStream.toByteArray();
//			return byteData;
//		} 
//		finally {
//			outStream.close();
//		}
//	}		
//	
//	private final static GenericRecord userToRecord(User user, Schema schema) {
//		GenericRecord datum = new GenericData.Record(schema);
//		datum.put("id", user.getId());
//		datum.put("name", user.getName());
//		datum.put("email", user.getEmail().get());
//		datum.put("previousAddresses", user.getPreviousAddresses());
//		return datum;
//	}
//
//	private final static GenericRecord beanToRecord(AllDataTypesBean bean, Schema schema) {
//		GenericRecord genericUser = new GenericData.Record(schema);
//		//		genericUser.put("enumeration", bean.getEnumeration());
////		genericUser.put("anyNullable", bean.getAnyNullable());
//		genericUser.put("anyOption", bean.getAnyOption().get());
//		//		genericUser.put("byteArray", bean.getByteArray());
//		//		genericUser.put("arrayAny", bean.getArrayAny());
//		genericUser.put("setAny", bean.getSetAny());
//		genericUser.put("fixedValue", new Fixed(bean.getFixedValue()));
//		genericUser.put("seqAny", bean.getSeqAny());
//		genericUser.put("mapAny", bean.getMapAny());		      
//		genericUser.put("string", bean.getString());
//		genericUser.put("timestamp", bean.getTimestamp().getTime());
//		genericUser.put("date", bean.getDate());
//		genericUser.put("bigDecimal", ByteBuffer.wrap(bean.getBigDecimal().unscaledValue().toByteArray()));
//		genericUser.put("bigInteger", ByteBuffer.wrap(bean.getBigInteger().toByteArray()));	
////		genericUser.put("decimal", bean.getDecimal().toString());
//		genericUser.put("int", bean.getIntValue());
//		genericUser.put("double", bean.getDoubleValue());
//		genericUser.put("float", bean.getFloatValue());
//		genericUser.put("short", bean.getShortValue());
//		genericUser.put("byte", bean.getByteValue());
//		genericUser.put("boolean", bean.isBooleanValue());		
//		
//		GenericRecord oneMore = new GenericData.Record(schema.getField("nested").schema().getField("nestedValue").schema());
//		oneMore.put("lastNest", bean.getNested().getNestedValue().getLastNest());
//		
//		GenericRecord nested = new GenericData.Record(schema.getField("nested").schema());
//		nested.put("nestedKey", bean.getNested().getNestedKey());
//		nested.put("nestedValue", oneMore);
//		
//		genericUser.put("nested", nested);
//		
//		return genericUser;
//	}	
//
//	private final static KafkaProducer<String,byte[]> createKafkaProducer() {
//		Properties properties = new Properties();
//		properties.put("bootstrap.servers", GlobalSettings.METADATA_BROKER_LIST);
//		properties.put("metadata.broker.list", GlobalSettings.METADATA_BROKER_LIST);
//		properties.put("client.id", UUID.randomUUID().toString());
//		properties.put("acks","all");
//		properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
//		properties.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
//		properties.put("block.on.buffer.full","false");		
//
//		return new KafkaProducer<String, byte[]>(properties);      
//	}
//
//	private final static DatumWriter<GenericRecord> createDatumWriter(Schema schema) {		
//		return new GenericDatumWriter<GenericRecord>(schema);
//	}
//
//	private final static Schema createSchema(File sourcePath) throws Exception {
//		return new Schema.Parser().parse(sourcePath);
//	}
//
//	private final static User createUser(int id, Random random) {
//		return new User(id, 
//				nextRandomString(10, random), 
//				Optional.of(nextRandomString(10, random) + "@" + nextRandomString(10, random)), 
//				createRandomAddresses(random.nextInt(3), random));
//	}
//
//	private final static String nextRandomString(int length, Random random) {
//		StringBuilder charAcc = new StringBuilder();
//		for (int i = 0; i < length; i++) {
//			charAcc.append((char) (97 + random.nextInt(26)));
//		}
//		return charAcc.toString();
//	}
//
//	private final static List<String> createRandomAddresses(int quantity, Random random) {
//
//		List<String> randomAddresses = new ArrayList<String>();
//		for (int i = 0 ; i < quantity ; i++) {
//			randomAddresses.add(nextRandomString(5, random) + " " + nextRandomString(10, random));
//		}
//		return randomAddresses;
//	}
//
//	private final static File getSchemaFromResources(String path) {		
//		ClassLoader classLoader = AvroWriter.class.getClassLoader();
//		return new File(classLoader.getResource(path).getFile());		
//	}
//	
//	private final static class Fixed implements GenericFixed {
//
//		private final ByteBuffer value;
//		public Fixed(ByteBuffer value) {
//			this.value = value;					
//		}		
//		@Override
//		public Schema getSchema() {		
//			return null;
//		}
//		@Override
//		public byte[] bytes() {
//			return this.value.array();
//		}		
//	}
} 
