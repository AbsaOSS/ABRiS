

# ABRiS - Avro Bridge for Spark

- Pain free Spark/Avro integration.

- Seamlessly convert your Avro records from anywhere (e.g. Kafka, Parquet, HDFS, etc) into Spark Rows. 

- Convert your Dataframes into Avro records without even specifying a schema.

- Seamlessly integrate with Confluent platform, including Schema Registry with all available [naming strategies](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#how-the-naming-strategies-work) and schema evolution.

- Go back-and-forth Spark Avro (since Spark 2.4).


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

Detailed instructions for many use cases are in separated documents:

- [How to use Abris with vanilla avro (with examples)](documentation/vanilla-avro-documentation.md)
- [How to use Abris with Confluent avro (with examples)](documentation/confluent-avro-documentation.md)
- [How to use Abris in Python (with examples)](documentation/python-documentation.md)

Full runnable examples can be found in the ```za.co.absa.abris.examples``` package. You can also take a look at unit tests in package ```za.co.absa.abris.avro.sql```.

**IMPORTANT**: Spark dependencies have `provided` scope in the `pom.xml`, so when running the examples, please make sure that you either, instruct your IDE to include dependencies with 
`provided` scope, or change the scope directly.

### Confluent Avro format    
The format of Avro binary data is defined in [Avro specification](http://avro.apache.org/docs/current/spec.html). Confluent format extends it and prepends the schema id before the actual record. The Confluent expressions in this library expect this format and add the id after the Avro data are generated or remove it before they are parsed.

You can find more about Confluent and Schema Registry in [Confluent documentation](https://docs.confluent.io/current/schema-registry/index.html).


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

## Other Features

### Using schema manager to directly download or register schema
You can use SchemaManager directly to download or upload a schema. The configuration is identical to one use for the rest of Abris.
The SchemaManager API may be changed from version to version. It's still considered to be internal object of the library.

```scala
// Downloading schema:

val schemaRegistryConfig = Map( ...configuration... )

val schemaManager = SchemaManagerFactoryg.create(schemaRegistryConfig)
val schema = schemaManager.downloadSchema()
```

```scala
// Registering schema:

val schemaString = "...schema..."
val schemaRegistryConfig = Map( ...configuration... )

val schemaManager = SchemaManagerFactory.create(schemaRegistryConfig)
val schemaId = schemaManager.register(schemaString)
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

---

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
