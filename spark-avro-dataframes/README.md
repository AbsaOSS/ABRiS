    Copyright 2018 Barclays Africa Group Limited
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

# ABRIS - Avro Bridge for Spark

Pain free Spark/Avro integration.

Seamlessly convert your Avro records from anywhere (e.g. Kafka, Parquet, HDFS, etc) into Spark Rows. 

Convert your Dataframes into Avro records without even specifying a schema.


## Motivation

Among the motivations for this project, it is possible to highlight:

- Avro is Confluent's recommended payload to Kafka (https://www.confluent.io/blog/avro-kafka-data/).

- Current solutions do not support reading Avro record as Spark structured streams (e.g. https://github.com/databricks/spark-avro).

- Lack tools for writing Spark Dataframes directly into Kafka as Avro records.

- Right now, all the efforts to integrated Avro-Spark streams or Dataframes depend on the user.


## Usage

### Reading Avro binary records from Kafka as Spark structured streams and performing regular SQL queries on them

1. Import the library: ```import za.co.absa.abris.avro.AvroSerDe._```

2. Open a Kafka connection into a structured stream: ```spark.readStream.format("kafka"). ...```

3. Invoke the library on your structured stream: ```... .avro("path_to_Avro_schema")```

4. Pass on your query and start the stream: ```... .select("your query").start().awaitTermination()``` 

Below is an example whose full version can be found at ```za.co.absa.abris.examples.SampleKafkaAvroFilterApp```

```scala
    val spark = SparkSession
      .builder()
      .appName("ReadAvro")
      .master("local[2]")
      .getOrCreate()    
      
    // import Spark Avro Dataframes
    import za.co.absa.abris.avro.AvroSerDe._

    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test-topic")
      .fromAvro("path_to_Avro_schema") // invoke the library

    stream.filter("field_x % 2 == 0")
      .writeStream
      .format("console")
      .start()
      .awaitTermination() 
```

### Writing Dataframes to Kafka as Avro records specifying a schema

1. Create your Dataframe. It MUST have a SQL schema.

2. Import the library: ```import za.co.absa.abris.avro.AvroSerDe._```

3. Invoke the library informing the path to the Avro schema to be used to generate the records: ```dataframe.avro("path_to_existing_Avro_schema")```

4. Send your data to Kafka as Avro records: ```... .write.format("kafka") ...```

Below is an example whose full version can be found at ```za.co.absa.abris.examples.SampleKafkaDataframeWriterApp```

```scala
    val spark = SparkSession
      .builder()
      .appName("KafkaAvroWriter")
      .master("local[2]")
      .getOrCreate()                 
            
      import spark.implicits._
      
      // import library
      import za.co.absa.abris.avro.AvroSerDe._
      
      val sparkSchema = StructType( .... // your SQL schema
      implicit val encoder = RowEncoder.apply(sparkSchema)
      val dataframe = spark.parallelize( .....
      
      dataframe
      	.toAvro("path_to_existing_Avro_schema") // invoke library            
      	.write
      	.format("kafka")    
      	.option("kafka.bootstrap.servers", "localhost:9092"))
      	.option("topic", "test-topic")
      	.save()  
```


### Writing Dataframes to Kafka as Avro records without specifying a schema

Follow the same steps as above, however, when invoking the library, instead of informing the path to the Avro schema, you can just inform the expected name and namespace for the records, and the library will infer the complete schema from the Dataframe: ```dataframe.avro("schema_name", "schema_namespace")```

Below is an example whose full version can be found at ```za.co.absa.abris.examples.SampleKafkaDataframeWriterApp```

```scala
    val spark = SparkSession
      .builder()
      .appName("KafkaAvroWriter")
      .master("local[2]")
      .getOrCreate()                 
            
      import spark.implicits._
            
      val sparkSchema = StructType( .... // your SQL schema
      implicit val encoder = RowEncoder.apply(sparkSchema)
      val dataframe = spark.parallelize( .....
      
      // import library
      import za.co.absa.abris.avro.AvroSerDe._
      
      dataframe
      	.toAvro("dest_schema_name", "dest_schema_namespace") // invoke library            
      	.write
      	.format("kafka")    
      	.option("kafka.bootstrap.servers", "localhost:9092"))
      	.option("topic", "test-topic")
      	.save()         
```

## Other Features

### Data Conversions
This library also provides convenient methods to convert between Avro and Spark schemas. 

If you have an Avro schema which you want to convert into a Spark SQL one - to generate your Dataframes, for instance - you can do as follows: 

```scala
val avroSchema: Schema = AvroSchemaUtils.load("path_to_avro_schema")
val sqlSchema: StructType = SparkAvroConversions.toSqlType(avroSchema) 
```  

You can also do the inverse operation by running:

```scala
val sqlSchema = new StructType(new StructField ....
val avroSchema = SparkAvroConversions.toAvroSchema(sqlSchema, avro_schema_name, avro_schema_namespace)
```

### Alternative Data Sources
Your data may not come from Spark and, if you are using Avro, there is a chance you are also using Java. This library provides utilities to easily convert Java beans into Avro records and send them to Kafka. 

To use you need to: 

1. Store the Avro schema for the class structure you want to send (the Spark schema will be inferred from there later, in your reading job):

```Java
Class<?> dataType = YourBean.class;
String schemaDestination = "/src/test/resources/DestinationSchema.avsc";
AvroSchemaGenerator.storeSchemaForClass(dataType, Paths.get(schemaDestination));
```

2. Write the configuration to access your cluster:

```Java
Properties config = new Properties();
config.put("bootstrap.servers", "...
...
```

3. Create an instance of the writer:

```Java
KafkaAvroWriter<YourBean> writer = new KafkaAvroWriter<YourBean>(config);
```

4. Send your data to Kafka:

```Java
List<YourBean> data = ...
long dispatchWait = 1l; // how much time the writer should expect until all data are dispatched
writer.write(data, "destination_topic", dispatchWait);
```

A complete application can be found at ```za.co.absa.abris.utils.examples.SimpleAvroDataGenerator``` under Java source.

## Dependencies

The environment dependencies are below. For the other dependencies, the library POM is configured with all dependencies scoped as ```compile```, thus, you can understand it as a self-contained piece of software. In case your environment already provides some of those dependencies, you can specify it in your project POM.

- Scala 2.11

- Spark 2.2.0

- Spark SQL Kafka 0-10

- Spark Streaming Kafka 0-8 or higher


## Performance

### Setup

- Windows 7 Enterprise 64-bit 

- Intel i5-3550 3.3GHz

- 16 GB RAM


Tests serializing 50k records using a fairly complex schemas show that Avro records can be up to 16% smaller than Kryo ones, i.e. converting Dataframes into Avro records save up to 16% more space than converting them into case classes and using Kryo as a serializer.

In local tests with a single core, the library was able to parse, per second, up to 100k Avro records into Spark rows per second, and up to 5k Spark rows into Avro records.

The tests can be found at ```za.co.absa.abris.performance.SpaceTimeComplexitySpec```.

 