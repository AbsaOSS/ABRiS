/*
 * Copyright 2018 ABSA Group Limited
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

package za.co.absa.abris.utils.avro.kafka.write;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import za.co.absa.abris.utils.avro.AvroPayloadConverter;

public class KafkaAvroWriter<T> {

	private final static Logger logger = LoggerFactory.getLogger(KafkaAvroWriter.class);
	
	private final static String PROP_BOOTSTRAP_SERVERS = "bootstrap.servers";
	private final static String PROP_METADATA_BROKER_LIST = "metadata.broker.list";
	private final static String PROP_CLIENT_ID = "client.id";
	private final static String PROP_ACKS = "acks";
	private final static String PROP_KEY_SERIALIZERS = "key.serializer";
	private final static String PROP_VALUE_SERIALIZER = "value.serializer";	

	private final KafkaProducer<String, byte[]> kafkaSender;
	private final AvroPayloadConverter avroConverter;

	public KafkaAvroWriter(Properties connectionProps) {
		if (!validate(connectionProps)) {
			throw new IllegalArgumentException("Missing Kafka connection parameters.");
		}		
		
		this.kafkaSender = new KafkaProducer<String, byte[]>(connectionProps);	
		this.avroConverter = new AvroPayloadConverter();
	}

	private final boolean validate(Properties properties) {
		return contains(properties, PROP_BOOTSTRAP_SERVERS) && 
				contains(properties, PROP_METADATA_BROKER_LIST) &&
				contains(properties, PROP_CLIENT_ID) &&
				contains(properties, PROP_ACKS) &&
				contains(properties, PROP_KEY_SERIALIZERS) &&
				contains(properties, PROP_VALUE_SERIALIZER);
	}

	private final boolean contains(Properties properties, String name) {		
		if (!properties.containsKey(name)) {
			logger.error("Missing property: "+name);
			return false;
		}		
		return true;
	}

	/**
	 * Writes a list of data beans into Kafka. The data MUST be in a Java beans-compliant format.
	 * Throws if list is empty.
	 */
	public final int write(List<T> data, String[] topics, long timeoutSecs) {
		if (data.isEmpty()) {
			throw new IllegalArgumentException("Empty data list.");
		}
		Objects.requireNonNull(topics, "Empty list of topics.");		
		
		int sent = 0;
		for (T t : data) {
			if (this.write(t, topics)) {
				sent++;
			}
		}

		this.waitFor(timeoutSecs);

		logger.info("Sent "+sent+" of "+data.size()+" entries to '"+this.toString(topics)+"'.");
		return sent;
	}

	private final String toString(String[] topics) {
		StringBuilder allTopics = new StringBuilder();
		for (String topic : topics) {
			allTopics.append(topic).append(", ");
		}
		return allTopics.toString();
	}
	
	private final boolean write(Object o, String[] topics) {

		for (String topic : topics) {
			try {
				byte[] payload = this.avroConverter.toAvroPayload(o);
				this.send(payload, topic);	
				logger.info("Message sent to topic: "+topic);
			}
			catch (Exception e) {
				e.printStackTrace();
				return false;
			}
		}
		return true;
	}

	private final void send(byte[] payload, String topics) throws InterruptedException, ExecutionException, TimeoutException {
		ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topics, payload);                
		this.kafkaSender.send(record);				
	}		

	private final void waitFor(long secs) {
		try {
			Thread.sleep(secs * 1000);
		} catch (InterruptedException e) {			
			e.printStackTrace();
		}
	}
}
