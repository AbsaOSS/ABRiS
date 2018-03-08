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

package za.co.absa.avro.dataframes.utils.avro.kafka;

public class Settings {
	  public final static String METADATA_BROKER_LIST = "localhost:9092";
//	  public final static String METADATA_BROKER_LIST = "abcap-kafka-uat-3.barcapint.com:9092";	  
//	  public final static String METADATA_BROKER_LIST = "PLAINTEXT://abcap-kafka-uat-3.barcapint.com:9092";
	  
	  public final static String MESSAGE_SEND_MAX_RETRIES = "5";
	  public final static String REQUEST_REQUIRED_ACKS = "-1";
	  public final static String SERIALIZER_CLASS = "kafka.serializer.DefaultEncoder";
	  public final static String ZOOKEEPER_CONNECT = "localhost:2181";
	  public final static String AUTO_OFFSET_RESET = "smallest";
	  public final static String CONSUMER_TIME_MS = "120000";
	  public final static String AUTO_COMMIT_INTERVAL_MS = "10000";
	  
	  public final static String[] TOPICS = {"avro-dataframes-topic"};
}
