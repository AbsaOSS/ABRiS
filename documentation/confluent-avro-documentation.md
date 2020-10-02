# ABRiS - Confluent Avro documentation

- [Avro to Spark](#Avro-to-Spark)
- [Spark to Avro](#spark-to-avro)

The main difference between confluent avro and vanilla Avro is in whether it expects the schema id in the Avro payload. 
In Confluent avro there always have to be schema id on the start of the payload. 

## Avro to Spark
When converting from Confluent avro to Spark, there may be two schemas *reader schema* and *writer schema*. 
 - Writer schema is the one used to convert data to avro and is the one identified by id in the avro payload.
 - Reader schema is the one specified by you.
 
 The schemas must be compatible.
 
There are several ways how to get the reader schema, most of them are in the following config examples:

```scala
  val fromAvroConfig1: FromAvroConfig = AbrisConfig
    .fromConfluentAvro
    .provideReaderSchema("{ ...schema json...}")
    .usingSchemaRegistry("http://registry-url")
  
  // or
  val fromAvroConfig2: FromAvroConfig = AbrisConfig
    .fromConfluentAvro
    .downloadReaderSchemaById(66)
    .usingSchemaRegistry("http://registry-url")

  //or
  val fromAvroConfig3: FromAvroConfig = AbrisConfig
    .fromConfluentAvro
    .downloadReaderSchemaByLatestVersion
    .andTopicNameStrategy("topicName", true) // key schema
    .usingSchemaRegistry("http://registry-url")
```
Once you have 'FromAvroConfig' you just need to pass it to Abris function:
```scala
import za.co.absa.abris.avro.functions.from_avro

def readAvro(dataFrame: DataFrame, fromAvroConfig: FromAvroConfig): DataFrame = {

  dataFrame.select(from_avro(col("value"), fromAvroConfig) as 'data).select("data.*")
}
```

## Spark to Avro
When converting data to Avro there is only one schema in play, but you have several options how to provide it:
 - You can provide it as a string and let Abris register the schema for you.
 - You can specify a schema that already is in the registry. In that case Abris will download it and no registration is necessary.

When registering the schema Abris will do it only if the same schema is not already registered. 
So it's something like: register if not exist.

Some configuration examples:
```scala
val toAvroConfig1: ToAvroConfig = AbrisConfig
    .toConfluentAvro
    .provideAndRegisterSchema("{ ...schema json... }")
    .usingRecordNameStrategy() // name and namspce taken from schema
    .usingSchemaRegistry("http://registry-url")

val toAvroConfig2: ToAvroConfig = AbrisConfig
    .toConfluentAvro
    .provideAndRegisterSchema("{ ...schema json... }")
    .usingTopicNameStrategy("fooTopic")
    .usingSchemaRegistry("http://registry-url")

val toAvroConfig3: ToAvroConfig = AbrisConfig
    .toConfluentAvro
    .downloadSchemaById(66)
    .usingSchemaRegistry("http://registry-url")

val toAvroConfig4: ToAvroConfig = AbrisConfig
    .toConfluentAvro
    .downloadSchemaByLatestVersion
    .andTopicNameStrategy("fooTopic")
    .usingSchemaRegistry("http://registry-url")
```
Once you have a config you can use it like this:
```scala
import za.co.absa.abris.avro.functions.to_avro

def writeAvro(dataFrame: DataFrame, toAvroConfig: ToAvroConfig): DataFrame = {
  
  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
  dataFrame.select(to_avro(allColumns, toAvroConfig) as 'value)
}
```
