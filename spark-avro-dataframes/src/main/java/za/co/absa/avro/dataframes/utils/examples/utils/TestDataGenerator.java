package za.co.absa.avro.dataframes.utils.examples.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import za.co.absa.avro.dataframes.utils.avro.data.annotations.NestedAvroData;

@SuppressWarnings("unused")
public class TestDataGenerator {
		
	public final static Class<?> getContainerClass() {
		return TestData.class;
	}
	
	public final static List<TestData> generate(int batchSize) {
		
		Random random = new Random();
		List<TestData> testData = new ArrayList<TestData>();
		
		for (int i = 0 ; i < batchSize ; i++) {
			testData.add(new TestData(random.nextInt(), 
					random.nextFloat(), 
					random.nextLong(), 
					random.nextDouble(), 
					UUID.randomUUID().toString(), 
					createRandomList(random.nextInt(10), random), 
					createRandomSet(random.nextInt(10), random), 
					createRandomMap(random.nextInt(10), random), 
					createRandomComplex(random.nextInt(10), random)));
		}		
		
		return testData;
	}
	
	private final static ComplexNested createRandomComplex(int count, Random random) {		
		return new ComplexNested(random.nextLong(), 
				new SimpleData(random.nextInt(), 
						UUID.randomUUID().toString(), 
						createRandomList(random.nextInt(10), random)));
	}
	
	private final static Map<String,Integer> createRandomMap(int entries, Random random) {
		Map<String,Integer> randomMap = new HashMap<String,Integer>();
		for (int i = 0 ; i < entries ; i++) {
			randomMap.put(UUID.randomUUID().toString(), random.nextInt());
		}
		return randomMap;
	}
	
	private final static List<Long> createRandomList(int entries, Random random) {
		List<Long> randomList = new ArrayList<Long>();
		for (int i = 0 ; i < entries ; i++) {
			randomList.add(random.nextLong());
		}
		return randomList;
	}

	private final static Set<Long> createRandomSet(int entries, Random random) {
		Set<Long> randomSet = new HashSet<Long>();
		for (int i = 0 ; i < entries ; i++) {
			randomSet.add(random.nextLong());
		}
		return randomSet;
	}	
		
	public final static class TestData {
		private int anInt;
		private float aFloat;
		private long aLong;
		private double aDouble;
		private String aString;
		private List<Long> aList;
		private Set<Long> aSet;
		private Map<String,Integer> mapAny;				
		private ComplexNested complexNested;
		public TestData(int anInt, float aFloat, long aLong, double aDouble, String aString, List<Long> aList,
				Set<Long> aSet, Map<String, Integer> aMap, ComplexNested complexNested) {			
			this.anInt = anInt;
			this.aFloat = aFloat;
			this.aLong = aLong;
			this.aDouble = aDouble;
			this.aString = aString;
			this.aList = aList;
			this.aSet = aSet;
			this.mapAny = aMap;			
			this.complexNested = complexNested;
		}			
	}	
	
	@NestedAvroData
	public final static class ComplexNested {
		
		private long whateverLong;
		private SimpleData nested;
		
		public ComplexNested(long whateverLong, SimpleData nested) {		
			this.whateverLong = whateverLong;
			this.nested = nested;
		}		
	}
	
	@NestedAvroData
	public final static class SimpleData {
		private int id;
		private String name;		
		private List<Long> values;
		
		public SimpleData(int id, String name, List<Long> values) {		
			this.id = id;
			this.name = name;
			this.values = values;
		}		
	}		
}
