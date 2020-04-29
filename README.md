    Copyright 2018 ABSA Group Limited
    
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

# ABRiS - Avro Bridge for Spark

Pain free Spark/Avro integration.

Seamlessly convert your Avro records from anywhere (e.g. Kafka, Parquet, HDFS, etc) into Spark Rows. 

Convert your Dataframes into Avro records without even specifying a schema.

Seamlessly integrate with Confluent platform, including Schema Registry with all available [naming strategies](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#how-the-naming-strategies-work).

Go back-and-forth Spark Avro (since Spark 2.4).


### Coordinates for Maven POM dependency
#### Abris for Scala 2.11
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/abris_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/abris_2.11)

#### Abris for Scala 2.12
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/abris_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/abris_2.12)

## Supported Spark versions
On spark 2.4.x Abris should work without any further requirements.

On Spark 2.3.x you must declare dependency on ```org.apache.avro:avro:1.8.0``` or higher. (Spark 2.3.x uses Avro 1.7.x so you must overwrite this because ABRiS needs Avro 1.8.0+.)

## Usage

ABRiS API is in it's most basic form almost identical to Spark built-in support for Avro, but it provides additional functionality. Mainly it's support of schema registry and also seamless integration with confluent Avro data format.

The API consists of four Spark SQL expressions: 
* ```to_avro``` and ```from_avro``` used for normal Avro payload
* ```to_confluent_avro``` and ```from_confluent_avro``` used for Confluent Avro data format

Full runnable examples can be found in the ```za.co.absa.abris.examples``` package. You can also take a look at unit tests in package ```za.co.absa.abris.avro.sql```.

**IMPORTANT**: Spark dependencies have `provided` scope in the `pom.xml`, so when running the examples, please make sure that you either, instruct your IDE to include dependencies with 
`provided` scope, or change the scope directly.

### Deprecation Note
Old ABRiS API is deprecated, but is still included in the library. Documentation for old API is in [ABRiS 2.2.3.](https://github.com/AbsaOSS/ABRiS/tree/v2.2.3)

### Confluent Avro format    
The format of Avro binary data is defined in [Avro specification](http://avro.apache.org/docs/current/spec.html). Confluent format extends it and prepends the schema id before the actual record. The Confluent expressions in this library expect this format and add the id after the Avro data are generated or remove it before they are parsed.

You can find more about Confluent and Schema Registry in [Confluent documentation](https://docs.confluent.io/current/schema-registry/index.html).

### Reading Avro binary records with provided Avro schema    
```scala
import za.co.absa.abris.avro.functions.from_avro

def readAvro(dataFrame: DataFrame, schemaString: String): DataFrame = {

  dataFrame.select(from_avro(col("value"), schemaString) as 'data).select("data.*")
}
```
In this example the Avro binary data are in ```dataFrame``` inside column the **value**. The Avro schema is provided as a string ```schemaString```.

After the Avro data are converted to Spark SQL representation they are stored in column the **data**. This column is immediately flattened in the next select so the result will be a ```DataFrame``` containing only the deserialized avro data.  

### Reading Avro binary records using Schema Registry
If you want to use Schema Registry you need to provide a configuration:

```scala
val schemaRegistryConfig = Map(
  SchemaManager.PARAM_SCHEMA_REGISTRY_URL          -> "url_to_schema_registry",
  SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC        -> "topic_name",
  SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.{TOPIC_NAME, RECORD_NAME, TOPIC_RECORD_NAME}, // choose a subject name strategy
  SchemaManager.PARAM_VALUE_SCHEMA_ID              -> "current_schema_id" // set to "latest" if you want the latest schema version to used  
  SchemaManager.PARAM_VALUE_SCHEMA_VERSION         -> "current_schema_version" // set to "latest" if you want the latest schema version to used
)
```
Depending on the selected naming strategy you may also need to provide ```SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY``` and ```SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY```
 
```scala
import za.co.absa.abris.avro.functions.from_avro

def readAvro(dataFrame: DataFrame, schemaRegistryConfig: Map[String, String]): DataFrame = {

  dataFrame.select(from_avro(col("value"), schemaRegistryConfig) as 'data).select("data.*")
}
```
This example is similar to the previous one except for the fact that this time configurations to access Schema Registry are provided instead of the schema itself. The schema must already be available on Schema Registry.

### Reading Confluent Avro binary records with provided Avro schema    
```scala
import za.co.absa.abris.avro.functions.from_confluent_avro

def readAvro(dataFrame: DataFrame, schemaString: String): DataFrame = {

  dataFrame.select(from_confluent_avro(col("value"), schemaString) as 'data).select("data.*")
}
```
The main difference between ```from_confluent_avro``` and ```from_avro``` is in whether it expects the schema_id in the Avro payload. The usage is identical to previous examples.

### Reading Confluent Avro binary records using schema registry
Schema Registry configuration is the same as in previous Schema Registry example.
```scala
import za.co.absa.abris.avro.functions.from_confluent_avro

def readAvro(dataFrame: DataFrame, schemaRegistryConfig: Map[String, String]): DataFrame = {

  dataFrame.select(from_confluent_avro(col("value"), schemaRegistryConfig) as 'data).select("data.*")
}
```
The only difference is the expression name.

       
### Reading Confluent Avro binary records using Schema Registry for key and value
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

### Writing Avro records 
```scala
import za.co.absa.abris.avro.functions.to_avro

def writeAvro(dataFrame: DataFrame): DataFrame = {

  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
  dataFrame.select(to_avro(allColumns) as 'value)
}
```
This is the simplest possible usage of the ```to_avro``` expression. We just provide the column that we want to serialize and the library will generate the schema automatically from Spark data types. 

If you want to serialize more than one column, you have to put them in a Spark *struct* first, as shown in the example.

### Writing Avro records with provided Avro schema 
```scala
import za.co.absa.abris.avro.functions.to_avro

def writeAvro(dataFrame: DataFrame, schemaString: String): DataFrame = {

  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
  dataFrame.select(to_avro(allColumns, schemaString) as 'value)
}
```
If you provide the Avro schema as a second argument, ABRiS will use it to convert Spark data into Avro. Please make sure that the data types in Spark DataFrame and in schema are compatible.

### Writing Avro records using schema registry
First we need to provide the Schema Registry configuration:
```scala
val schemaRegistryConfig = Map(
  SchemaManager.PARAM_SCHEMA_REGISTRY_URL                        -> "url_to_schema_registry",
  SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC                      -> "topic_name",
  SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY               -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME,
  SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY      -> "schema_name",
  SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "schema_namespace",
  SchemaManager.PARAM_VALUE_SCHEMA_ID                            -> "current_schema_id", // (optional) set to "latest" if you want the latest schema version to used
  SchemaManager.PARAM_VALUE_SCHEMA_VERSION                       -> "current_schema_version" // (optional) set to "latest" if you want the latest schema version to used
)
```
In this example the ```TOPIC_RECORD_NAME``` naming strategy is used, therefore we need to provide topic, name and namespace.

```scala
import za.co.absa.abris.avro.functions.to_avro

def writeAvro(dataFrame: DataFrame, schemaRegistryConfig: Map[String, String]): DataFrame = {

  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)  
  dataFrame.select(to_avro(allColumns, schemaRegistryConfig) as 'value)
}
```
Since we didn't provide a schema it will be generated automatically and then stored into Schema Registry.

### Writing Avro records using schema registry and providing a schema
The only difference from previous example is that we have one additional parameter for the schema.
```scala
import za.co.absa.abris.avro.functions.to_avro

def writeAvro(dataFrame: DataFrame, schemaString: String, schemaRegistryConfig: Map[String, String]): DataFrame = {

  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)  
  dataFrame.select(to_avro(allColumns, schemaString, schemaRegistryConfig) as 'value)
}
```

### Writing Confluent Avro binary records using Schema Registry
Schema Registry configuration is the same as in previous examples.
```scala
import za.co.absa.abris.avro.functions.to_confluent_avro

def writeAvro(dataFrame: DataFrame, schemaRegistryConfig: Map[String, String]): DataFrame = {
  
  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
  dataFrame.select(to_confluent_avro(allColumns, schemaRegistryConfig) as 'value)
}
```
The main difference between ```from_confluent_avro``` and ```from_avro``` is in whether it prepends the schema_id to the Avro payload. The usage is identical to previous examples.

### Writing Confluent Avro binary records with provided avro schema    
```scala
import za.co.absa.abris.avro.functions.to_confluent_avro

def writeAvro(dataFrame: DataFrame, schemaString: String, schemaRegistryConfig: Map[String, String]): DataFrame = {

  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
  dataFrame.select(to_confluent_avro(allColumns, schemaString, registryConfig) as 'value)
}
```

### Writing Confluent Avro binary records using Schema Registry for key and value
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


### Schema Registry security settings

Some settings are required when using Schema Registry, as explained above. However, there is more settings supported by Schema Registry as contained in ```io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig```.

Among those are ```basic.auth.user.info``` and ```basic.auth.credentials.source``` required for user authentication.

To make use of those, all that is required is to add them to the settings map as in the example below:

```scala
val commonRegistryConfig = Map(
  SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "example_topic",
  SchemaManager.PARAM_SCHEMA_REGISTRY_URL -> "http://localhost:8081"
)

val valueRegistryConfig = commonRegistryConfig +
  (SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> "topic.name")

val securityRegistryConfig = valueRegistryConfig + 
  ("basic.auth.credentials.source" -> "USER_INFO",
   "basic.auth.user.info" -> "srkey:srvalue")
```
 


### Using ABRiS with Python and PySpark

ABRiS can also be used with PySpark to deserialize Avro payloads from Confluent Kafka. For that, we need to convert Python object into JVM ones. The snippet below shows how it can be achieved.

```python
import logging, traceback
import requests
from pyspark.sql import Column
from pyspark.sql.column import *

jvm_gateway = spark_context._gateway.jvm
abris_avro  = jvm_gateway.za.co.absa.abris.avro
naming_strategy = getattr(getattr(abris_avro.read.confluent.SchemaManager, "SchemaStorageNamingStrategies$"), "MODULE$").TOPIC_NAME()        

schema_registry_config_dict = {"schema.registry.url": schema_registry_url,
                               "schema.registry.topic": topic,
                               "value.schema.id": "latest",
                               "value.schema.naming.strategy": naming_strategy}

conf_map = getattr(getattr(jvm_gateway.scala.collection.immutable.Map, "EmptyMap$"), "MODULE$")
    for k, v in schema_registry_config_dict.items():
        conf_map = getattr(conf_map, "$plus")(jvm_gateway.scala.Tuple2(k, v))
        
    deserialized_df = data_frame.select(Column(abris_avro.functions.from_confluent_avro(data_frame._jdf.col("value"), conf_map))
                      .alias("data")).select("data.*")
```


## Other Features

### Schema registration for subject into Schema Registry
This library also provides utility methods for registering schemas with topics into Schema Registry. Below is an example of how it can be done.

```scala
    val schemaRegistryConfs = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL                  -> "url_to_schema_registry",
      SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY         -> SchemaManager.SchemaStorageNamingStrategies.{TOPIC_NAME, RECORD_NAME, TOPIC_RECORD_NAME}, // if you are retrieving value schema
      SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY           -> SchemaManager.SchemaStorageNamingStrategies.{TOPIC_NAME, RECORD_NAME, TOPIC_RECORD_NAME}, // if your are retrieving key schema
      SchemaManager.PARAM_KEY_SCHEMA_NAME_FOR_RECORD_STRATEGY      -> "key_schema_name", // if you're using RecordName or TopicRecordName strategies for key
      SchemaManager.PARAM_KEY_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "key_schema_namespace", // if you're using RecordName or TopicRecordName strategies for key
      SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY      -> "value_schema_name", // if you're using RecordName or TopicRecordName strategies for value
      SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "value_schema_namespace" // if you're using RecordName or TopicRecordName strategies for value
    )
    SchemaManager.configureSchemaRegistry(schemaRegistryConfs)

    val topic = "example_topic"
    val subject = SchemaManager.getSubjectName(topic, false) // create a subject for the value

    val schema = AvroSchemaUtils.load("path_to_the_schema_in_a_file_system")

    val schemaId = SchemaManager.register(schema, subject)
```

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

## IMPORTANT - Note on Schema Registry naming strategies
The naming strategies RecordName and TopicRecordName allow for a topic to receive different payloads, i.e. payloads containing different schemas that do not have to be compatible, as explained [here](https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#subject-name-strategy).

However, currently, there is no way for Spark to change Dataframes schemas on the fly, thus, if incompatible schemas are used on the same topic, the job will fail. Also, it would be cumbersome to write jobs that shift between schemas.

A possible solution would be for ABRiS to create an uber schema from all schemas expected to be part of a topic, which will be investigated in future releases.

## Avro Fixed type
**Fixed** is an alternative way of encoding binary data in Avro. Unlike *bytes* type the fixed type doesn't store the length of the data in the payload, but in Avro schema itself.

The corresponding data type in Spark is **BinaryType**, but the inferred schema will always use bytes type for this kind of data. If you want to use the fixed type you must provide the appropriate Avro schema.
