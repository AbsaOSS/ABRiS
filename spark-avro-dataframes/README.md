#Spark Avro Dataframes

Seamlessly get your Avro records from Kakfa and query them as a regular Structured Streaming. Convert your Dataframes to Avro records without even specifying a schema.

## Usage

### Structured stream binary Avro records from Kafka and perform regular queries on them
```scala
    val spark = SparkSession
      .builder()
      .appName("ReadAvro")
      .master("local[2]")
      .getOrCreate()    
      
    // import Spark Avro Dataframes
    import za.co.absa.avro.dataframes.avro.AvroSerDe._

    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test-topic")
      .avro("path_to_Avro_schema") // invoke the library

    stream.filter("field_x % 2 == 0")
      .writeStream
      .format("console")
      .start()
      .awaitTermination() 
```

### Writing Dataframes to Kafka as Avro records specifying a schema
```scala
    val spark = SparkSession
      .builder()
      .appName("KafkaAvroWriter")
      .master("local[2]")
      .getOrCreate()                 
            
      import spark.implicits._
      
      // import library
      import za.co.absa.avro.dataframes.avro.AvroSerDe._
      
      val sparkSchema = StructType( .... // your SQL schema
      implicit val encoder = RowEncoder.apply(sparkSchema)
      val dataframe = spark.parallelize( .....
      
      dataframe
      	.avro("dest_schema_name", "dest_schema_namespace") // invoke library            
      	.write
      	.format("kafka")    
      	.option("kafka.bootstrap.servers", "localhost:9092"))
      	.option("topic", "test-topic")
      	.save()         
```



### Writing Dataframes to Kafka as Avro records without specifying a schema
```scala
    val spark = SparkSession
      .builder()
      .appName("KafkaAvroWriter")
      .master("local[2]")
      .getOrCreate()                 
            
      import spark.implicits._
      
      // import library
      import za.co.absa.avro.dataframes.avro.AvroSerDe._
      
      val sparkSchema = StructType( .... // your SQL schema
      implicit val encoder = RowEncoder.apply(sparkSchema)
      val dataframe = spark.parallelize( .....
      
      dataframe
      	.avro("path_to_existing_Avro_schema") // invoke library            
      	.write
      	.format("kafka")    
      	.option("kafka.bootstrap.servers", "localhost:9092"))
      	.option("topic", "test-topic")
      	.save()  
```

## Performance
Tests on fairly complex schemas show that Avro records can be up to 16% smaller than Kryo ones. 

In local tests with a single core, the library can parse, per second, up to 100k Avro records into Spark rows per second, and up to 5k Spark rows into Avro records. 