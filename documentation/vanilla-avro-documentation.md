# ABRiS - vanilla Avro documentation

- [Avro to Spark](#Avro-to-Spark)
- [Spark to Avro](#spark-to-avro)
## Avro to Spark

### Providing Avro schema    
```scala
import za.co.absa.abris.avro.functions.from_avro

def readAvro(dataFrame: DataFrame, schemaString: String): DataFrame = {

  dataFrame.select(from_avro(col("value"), schemaString) as 'data).select("data.*")
}
```
In this example the Avro binary data are in ```dataFrame``` inside column the **value**. 
The Avro schema is provided as a string ```schemaString```.

After the Avro data are converted to Spark SQL representation they are stored in column the **data**. 
This column is immediately flattened in the next select so the result will be a ```DataFrame``` containing only the deserialized avro data.  

### Using Schema Registry
First we need to provide the Schema Registry configuration:
```scala
val fromAvroConfig1: FromAvroConfig = AbrisConfig
    .fromSimpleAvro
    .downloadSchemaById(66)
    .usingSchemaRegistry("http://registry-url")

// or
val fromAvroConfig2: FromAvroConfig = AbrisConfig
    .fromSimpleAvro
    .downloadSchemaByLatestVersion
    .andTopicRecordNameStrategy("topic", "recordName", "namespace")
    .usingSchemaRegistry("http://registry-url")

// or
val fromAvroConfig3: FromAvroConfig = AbrisConfig
    .fromSimpleAvro
    .downloadSchemaByVersion(3)
    .andTopicNameStrategy("topicFoo", isKey=true) // Use isKey=true for the key schema and isKey=false for the value schema
    .usingSchemaRegistry("http://registry-url")

// ...
```
There are several ways how to configure this. 
Each step in configurator will offer you some options, and you just have to choose what you want to do.
At the end you should get an instance of `FromAvroConfig` that you can use like this:


```scala
import za.co.absa.abris.avro.functions.from_avro

def readAvro(dataFrame: DataFrame, fromAvroConfig: FromAvroConfig): DataFrame = {

  dataFrame.select(from_avro(col("value"), fromAvroConfig) as 'data).select("data.*")
}
```

## Spark to Avro

### Providing Avro schema 
```scala
import za.co.absa.abris.avro.functions.to_avro

def writeAvro(dataFrame: DataFrame, schemaString: String): DataFrame = {

  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
  dataFrame.select(to_avro(allColumns, schemaString) as 'value)
}
```
If you provide the Avro schema as a second argument, ABRiS will use it to convert Spark data into Avro. 
Please make sure that the data types in Spark DataFrame and in schema are compatible.

### Using schema registry
If you want to use Schema Registry you need to provide a configuration:

```scala
val toAvroConfig1: ToAvroConfig = AbrisConfig
    .toSimpleAvro
    .provideAndRegisterSchema(schemaString)
    .usingTopicRecordNameStrategy("fooTopic") // record name is taken from the schema
    .usingSchemaRegistry("http://registry-url")

// or
val toAvroConfig2: ToAvroConfig = AbrisConfig
    .toSimpleAvro
    .downloadSchemaByVersion(4)
    .andTopicNameStrategy("fooTopic")
    .usingSchemaRegistry("http://registry-url")

// or
val toAvroConfig3: ToAvroConfig = AbrisConfig
    .toSimpleAvro
    .downloadSchemaById(66)
    .usingSchemaRegistry("http://registry-url")

// ...
```
There are several ways how to configure this. 
Each step in configurator will offer you some options, and you just have to choose what you want to do.
At the end you should get an instance of `ToAvroConfig` that you can use like this:
```scala
import za.co.absa.abris.avro.functions.to_avro

def writeAvro(dataFrame: DataFrame, toAvroConfig: ToAvroConfig): DataFrame = {

  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)  
  dataFrame.select(to_avro(allColumns, toAvroConfig) as 'value)
}
```
