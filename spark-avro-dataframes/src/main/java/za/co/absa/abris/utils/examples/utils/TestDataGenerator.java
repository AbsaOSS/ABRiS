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

package za.co.absa.abris.utils.examples.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import za.co.absa.abris.utils.avro.data.annotations.NestedAvroData;

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
					createRandomComplex(random)));
		}		
		
		return testData;
	}			
	
	private final static ComplexNested createRandomComplex(Random random) {		
		return new ComplexNested(random.nextLong(), 
				createRandomSimpleDataList(random.nextInt(10), random),
				createRandomSimpleDataMap(random.nextInt(10), random));
	}

	private final static Map<String,List<SimpleData>> createRandomSimpleDataMapList(int number, Random random) {
		Map<String,List<SimpleData>> map = new HashMap<String,List<SimpleData>>();
		for (int i = 0 ; i < number ; i++) {
			map.put(UUID.randomUUID().toString(), createRandomSimpleDataList(random.nextInt(10), random));			
		}
		return map;
	}		
	
	private final static Map<String,SimpleData> createRandomSimpleDataMap(int number, Random random) {
		Map<String,SimpleData> map = new HashMap<String,SimpleData>();
		for (int i = 0 ; i < number ; i++) {
			map.put(UUID.randomUUID().toString(), createRandomSimpleData(random));			
		}
		return map;
	}	
	
	private final static List<SimpleData> createRandomSimpleDataList(int number, Random random) {
		List<SimpleData> list = new ArrayList<SimpleData>();
		for (int i = 0 ; i < number ; i++) {
			list.add(createRandomSimpleData(random));
		}
		return list;
	}
	
	private final static SimpleData createRandomSimpleData(Random random) {
		return new SimpleData(random.nextInt(), UUID.randomUUID().toString(), createRandomList(random.nextInt(10), random));
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
		private List<SimpleData> nestedList;
		private Map<String,SimpleData> nestedMap;
		private Map<String,List<SimpleData>> nestedMapList;
		
		public ComplexNested(long whateverLong, List<SimpleData> nestedList, Map<String,SimpleData> nestedMap) {		
			this.whateverLong = whateverLong;
			this.nestedList = nestedList;
			this.nestedMap = nestedMap;
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
