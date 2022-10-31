

# ABRiS - Avro Bridge for Spark

- Pain free Spark/Avro integration.

- Seamlessly integrate with Confluent platform, including Schema Registry with all available [naming strategies](https://docs.confluent.io/current/schema-registry/serializer-formatter.html#how-the-naming-strategies-work) and schema evolution.

- Seamlessly convert your Avro records from anywhere (e.g. Kafka, Parquet, HDFS, etc) into Spark Rows. 

- Convert your Dataframes into Avro records without even specifying a schema.

- Go back-and-forth Spark Avro (since Spark 2.4).


### Coordinates for Maven POM dependency

| Scala  | Abris   |
|:------:|:-------:|
| 2.11   | [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/abris_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/abris_2.11) |
| 2.12   | [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/abris_2.12/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/abris_2.12) |
| 2.13   | [![Maven Central](https://maven-badges.herokuapp.com/maven-central/za.co.absa/abris_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/za.co.absa/abris_2.13) |

## Supported versions

| Abris   | Spark         | Scala       |
|:-----:  |:-----:        |:-----:      |
| 6.2.0 - 6.x.x   | 3.2.1 - 3.2.x | 2.12 / 2.13 |
| 6.0.0 - 6.1.1   | 3.2.0         | 2.12 / 2.13 |
| 5.0.0 - 5.x.x   | 3.0.x / 3.1.x | 2.12        |
| 5.0.0 - 5.x.x   | 2.4.x         | 2.11 / 2.12 |

From version 6.0.0, ABRiS only supports Spark 3.2.x.

ABRiS 5.0.x is still supported for older versions of Spark (see [branch-5](https://github.com/AbsaOSS/ABRiS/tree/branch-5))

## Older Versions
This is documentation for Abris **version 6**. Documentation for older versions is located in corresponding branches:
[branch-5](https://github.com/AbsaOSS/ABRiS/tree/branch-5),
[branch-4](https://github.com/AbsaOSS/ABRiS/tree/branch-4),
[branch-3.2](https://github.com/AbsaOSS/ABRiS/tree/branch-3.2).

## Confluent Schema Registry Version
Abris by default uses Confluent client version 6.2.0.

## Usage

ABRiS API is in it's most basic form almost identical to Spark built-in support for Avro, but it provides additional functionality. 
Mainly it's support of schema registry and also seamless integration with confluent Avro data format.

The API consists of two Spark SQL expressions (`to_avro` and `from_avro`) and fluent configurator (`AbrisConfig`)

Using the configurator you can choose from four basic config types:
* `toSimpleAvro`, `toConfluentAvro`, `fromSimpleAvro` and `fromConfluentAvro`

And configure what you want to do, mainly how to get the avro schema.

Example of usage:
```Scala
val abrisConfig = AbrisConfig
  .fromConfluentAvro
  .downloadReaderSchemaByLatestVersion
  .andTopicNameStrategy("topic123")
  .usingSchemaRegistry("http://localhost:8081")

import za.co.absa.abris.avro.functions.from_avro
val deserialized = dataFrame.select(from_avro(col("value"), abrisConfig) as 'data)
```

Detailed instructions for many use cases are in separated documents:

- [How to use Abris with vanilla avro (with examples)](documentation/vanilla-avro-documentation.md)
- [How to use Abris with Confluent avro (with examples)](documentation/confluent-avro-documentation.md)
- [How to use Abris in Python (with examples)](documentation/python-documentation.md)

Full runnable examples can be found in the ```za.co.absa.abris.examples``` package. You can also take a look at unit tests in package ```za.co.absa.abris.avro.sql```.

**IMPORTANT**: Spark dependencies have `provided` scope in the `pom.xml`, so when running the examples, please make sure that you either, instruct your IDE to include dependencies with 
`provided` scope, or change the scope directly.

### Confluent Avro format    
The format of Avro binary data is defined in [Avro specification](http://avro.apache.org/docs/current/spec.html). 
Confluent format extends it and prepends the schema id before the actual record. 
The Confluent expressions in this library expect this format and add the id after the Avro data are generated or remove it before they are parsed.

You can find more about Confluent and Schema Registry in [Confluent documentation](https://docs.confluent.io/current/schema-registry/index.html).


### Schema Registry security and other additional settings

Only Schema registry client setting that is mandatory is the url, 
but if you need to provide more the configurer allows you to provide a whole map.

For example, you may want to provide `basic.auth.user.info` and `basic.auth.credentials.source` required for user authentication.
You can do it this way:

```scala
val registryConfig = Map(
  AbrisConfig.SCHEMA_REGISTRY_URL -> "http://localhost:8081",
  "basic.auth.credentials.source" -> "USER_INFO",
  "basic.auth.user.info" -> "srkey:srvalue"
)

val abrisConfig = AbrisConfig
  .fromConfluentAvro
  .downloadReaderSchemaByLatestVersion
  .andTopicNameStrategy("topic123")
  .usingSchemaRegistry(registryConfig) // use the map instead of just url
```

## Other Features

### Generating Avro schema from Spark data frame column
There is a helper method that allows you to generate schema automatically from spark column. 
Assuming you have a data frame containing column "input". You can generate schema for data in that column like this:
```scala
val schema = AvroSchemaUtils.toAvroSchema(dataFrame, "input")
```

### Using schema manager to directly download or register schema
You can use SchemaManager directly to do operations with schema registry. 
The configuration is identical to Schema Registry Client.
The SchemaManager is just a wrapper around the client providing helpful methods and abstractions.

```scala
val schemaRegistryClientConfig = Map( ...configuration... )
val schemaManager = SchemaManagerFactory.create(schemaRegistryClientConfig)

// Downloading schema:
val schema = schemaManager.getSchemaById(42)

// Registering schema:
val schemaString = "{...avro schema json...}"
val subject = SchemaSubject.usingTopicNameStrategy("fooTopic")
val schemaId = schemaManager.register(subject, schemaString)

// and more, check SchemaManager's methods
```

### De-serialisation Error Handling
There are 2 ways ABRiS handles de-serialisation errors:

#### FailFast (Default)
Given no provided de-serialisation handler, a failure will result in a spark exception being thrown 
and with the error being outputted. This is the default procedure.

#### SpecificRecordHandler
The second option requires providing a default record that will be outputted in the event of a failure.
This should be used as a flag to be deleted outside ABRiS that should mean the spark job will not stop. 
Beware however, a null or empty record will also result in an error so a record with a different input should be chosen.

This can be provided as such:
```scala
val abrisConfig = AbrisConfig
  .fromConfluentAvro
  .downloadReaderSchemaByLatestVersion
  .andTopicNameStrategy("topic123")
  .usingSchemaRegistry(registryConfig)
  .withSchemaConverter("custom")
  .withExceptionHandler(new SpecificRecordExceptionHandler(providedDefaultRecord))
```

This is only for confluent-based configuration, not for standard avro.


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

#### Custom data conversions
If you would like to use custom logic to convert from Avro to Spark, you can implement the `SchemaConverter` trait.
The custom class is loaded in ABRiS using the service provider interface (SPI), so you need to register your class in your
`META-INF/services` resource directory. You can then configure the custom class with its short name or the fully qualified name.

**Example**

Custom schema converter implementation
```scala
package za.co.absa.abris.avro.sql
import org.apache.avro.Schema
import org.apache.spark.sql.types.DataType

class CustomSchemaConverter extends SchemaConverter {
  override val shortName: String = "custom"
  override def toSqlType(avroSchema: Schema): DataType = ???
}
```

Provider configuration file `META-INF/services/za.co.absa.abris.avro.sql.SchemaConverter`:
```
za.co.absa.abris.avro.sql.CustomSchemaConverter
```

Abris configuration
```scala
val abrisConfig = AbrisConfig
  .fromConfluentAvro
  .downloadReaderSchemaByLatestVersion
  .andTopicNameStrategy("topic123")
  .usingSchemaRegistry(registryConfig)
  .withSchemaConverter("custom")
```

## Multiple schemas in one topic
The naming strategies RecordName and TopicRecordName allow for a one topic to receive different payloads, 
i.e. payloads containing different schemas that do not have to be compatible, 
as explained [here](https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#subject-name-strategy).

When you read such data from Kafka they will be stored as binary column in a dataframe, 
but once you convert them to Spark types they cannot be in one dataframe, 
because all rows in dataframe must have the same schema.

So if you have multiple incompatible types of avro data in a dataframe you must first sort them out to several dataframes.
One for each schema. Then you can use Abris and convert the avro data.

## How to measure code coverage
```shell
./mvn clean verify -Pcode-coverage,scala-2.12
or
./mvn clean verify -Pcode-coverage,scala-2.13
```
Code coverage reports will be generated on paths:
```
{local-path}\ABRiS\target\jacoco
```

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
