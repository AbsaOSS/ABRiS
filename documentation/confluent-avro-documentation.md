# ABRiS - Confluent Avro documentation

- [Avro to Spark](#Avro-to-Spark)
- [Spark to Avro](#spark-to-avro)

## Avro to Spark

### Providing Avro schema    
```scala
import za.co.absa.abris.avro.functions.from_confluent_avro

def readAvro(dataFrame: DataFrame, schemaString: String): DataFrame = {

  dataFrame.select(from_confluent_avro(col("value"), schemaString) as 'data).select("data.*")
}
```
The main difference between ```from_confluent_avro``` and ```from_avro``` is in whether it expects the schema_id in the Avro payload. The usage is identical to previous examples.

### Using schema registry
Schema Registry configuration is the same as in previous Schema Registry example.
```scala
import za.co.absa.abris.avro.functions.from_confluent_avro

def readAvro(dataFrame: DataFrame, schemaRegistryConfig: Map[String, String]): DataFrame = {

  dataFrame.select(from_confluent_avro(col("value"), schemaRegistryConfig) as 'data).select("data.*")
}
```
The only difference is the expression name.

       
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


### Using Schema Registry
Schema Registry configuration is the same as in previous examples.
```scala
import za.co.absa.abris.avro.functions.to_confluent_avro

def writeAvro(dataFrame: DataFrame, schemaRegistryConfig: Map[String, String]): DataFrame = {
  
  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
  dataFrame.select(to_confluent_avro(allColumns, schemaRegistryConfig) as 'value)
}
```
The main difference between ```from_confluent_avro``` and ```from_avro``` is in whether it prepends the schema_id to the Avro payload. The usage is identical to previous examples.

### Providing avro schema    
```scala
import za.co.absa.abris.avro.functions.to_confluent_avro

def writeAvro(dataFrame: DataFrame, schemaString: String, schemaRegistryConfig: Map[String, String]): DataFrame = {

  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
  dataFrame.select(to_confluent_avro(allColumns, schemaString, registryConfig) as 'value)
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

