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
 
### Using schema registry for reader schema
In this case the reader schema will be loaded from schema registry according to configuration.
The writer schema will be loaded from the same registry.

example config:
```scala
val config = Map(
  SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "http://localhost:8081"
  SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "example_topic",
  SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> "topic.name",
  SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest"
)
```
the function:
```scala
import za.co.absa.abris.avro.functions.from_confluent_avro

def readAvro(dataFrame: DataFrame, schemaRegistryConfig: Map[String, String]): DataFrame = {

  dataFrame.select(from_confluent_avro(col("value"), schemaRegistryConfig) as 'data).select("data.*")
}
```
 
### Providing Avro schema
Other option is to provide the reader schema as a string. In that case the registry is used only for reader schema. 

example config:
```scala
val config = Map(
  SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "http://localhost:8081"
)
```
the function:
```scala
import za.co.absa.abris.avro.functions.from_confluent_avro

def readAvro(dataFrame: DataFrame, schemaString: String, config: Map[String, String]): DataFrame = {

  dataFrame.select(from_confluent_avro(col("value"), schemaString, config) as 'data).select("data.*")
}
```
       
### Using Schema Registry for key and value
In case we are sending Avro data using Kafka we may want to serialize both the key and the value of the Kafka message.
The serialization of Avro data is not really different when we are doing it for key or for value, but Schema Registry handles each of them slightly differently.

The way the library knows whether you are working with key or value is the schema naming strategy.
Use ```SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY``` for key and ```SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY``` for value. If the configuration contains **both** of them, it will **throw**!

This is one way to create the configurations for key and value serialization:
```scala
val commonRegistryConfig = Map(
  SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "example_topic",
  SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "http://localhost:8081"
)

val keyRegistryConfig = commonRegistryConfig ++ Map(
  SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> "topic.record.name",
  SchemaManager.PARAM_KEY_SCHEMA_ID -> "latest",
  SchemaManager.PARAM_KEY_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "foo",
  SchemaManager.PARAM_KEY_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "com.bar"
)

val valueRegistryConfig = commonRegistryConfig ++ Map(
  SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> "topic.name",
  SchemaManager.PARAM_VALUE_SCHEMA_ID -> "latest"
)
```
Let's assume that the Avro binary data for key are in the ```key``` column and the payload data are in the ```value``` column of the same DataFrame.

```scala
import za.co.absa.abris.avro.functions.from_confluent_avro

val result: DataFrame  = dataFrame.select(
    from_confluent_avro(col("key"), keyRegistryConfig) as 'key,
    from_confluent_avro(col("value"), valueRegistryConfig) as 'value)
```
We just need to use the right configuration for the right column and that's it.



## Spark to Avro
When converting data to Avro there is only one schema in play, but you have several options how to provide it:
 - You can provide it as a string or let Abris to generate it from data. In both cases the schema will be at the end registered to the registry.
 - You can specify a schema that already is in the registry. In that case Abris will download it and no registration is necessary.

In `config` map you have to provide a schema registry configuration and strategy to create subject for a registered schema. 
The config keys are identical to the ones used in previous examples.

### Generate schema from data and register
```scala
import za.co.absa.abris.avro.functions.to_confluent_avro

def writeAvro(dataFrame: DataFrame, config: Map[String, String]): DataFrame = {
  
  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
  dataFrame.select(to_confluent_avro(allColumns, config) as 'value)
}
```

### Use schema already in registry
The code is identical to previous example. The only difference is the config that must contain `SchemaManager.PARAM_KEY_SCHEMA_ID` or `SchemaManager.PARAM_KEY_SCHEMA_VERSION`.
In that case Abris will not register the schema. It will just use one identified by id or version and subject.

### Providing avro schema string
Provided schema will be registered.    
```scala
import za.co.absa.abris.avro.functions.to_confluent_avro

def writeAvro(dataFrame: DataFrame, schemaString: String, config: Map[String, String]): DataFrame = {

  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
  dataFrame.select(to_confluent_avro(allColumns, schemaString, config) as 'value)
}
```

### Using Schema Registry for key and value
As in the reading example, for writing you also have to specify the naming strategy parameter to let ABRiS know if you are using *value* or *key*.

```scala
val commonRegistryConfig = Map(
  SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "example_topic",
  SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "http://localhost:8081",
  SchemaManager.PARAM_KEY_SCHEMA_NAME_FOR_RECORD_STRATEGY -> "foo",
  SchemaManager.PARAM_KEY_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "com.bar"
)

val keyRegistryConfig = commonRegistryConfig +
  (SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY -> "topic.record.name")

val valueRegistryConfig = commonRegistryConfig +
  (SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> "topic.name")
```
Let's assume that we have the data that we want to serialize in a DataFrame in the **key** and **value** columns.

```scala
val result: DataFrame = dataFrame.select(
  to_confluent_avro(col("key"), keyRegistryConfig) as 'key,
  to_confluent_avro(col("value"), valueRegistryConfig) as 'value)
```
After serialization the data are again stored in the columns *key* and *value*, but now they are in Avro binary format.

