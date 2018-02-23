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
	  
//	  public final static String SHARED_TOPICS = "demo-topic-different-stuff-sdf";
//	  public final static String SHARED_TOPICS = "felipe-avro-dataframes";
	  public final static String TOPICS = "avro-dataframes-topic";
}
